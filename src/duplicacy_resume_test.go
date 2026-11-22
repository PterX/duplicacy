// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

// mockResumeStorage is an in-memory Storage implementing RangeDownloadStorage for deterministic
// testing of chunk download resume, fallback, 416 handling, FindChunk retries, and local caching.
type mockResumeStorage struct {
	StorageBase

	mu    sync.Mutex
	files map[string][]byte

	downloadCalls      int
	downloadRangeCalls int
	rangeOffsets       []int64
	getFileInfoCalls   int

	onGetFileInfo   func(call int, filePath string) error
	onDownload      func(call int, filePath string, data []byte, chunk *Chunk) error
	onDownloadRange func(call int, filePath string, offset int64, data []byte, chunk *Chunk) error
}

func newMockResumeStorage() *mockResumeStorage {
	s := &mockResumeStorage{
		files: make(map[string][]byte),
	}
	s.DerivedStorage = s
	s.SetDefaultNestingLevels([]int{1}, 1)
	return s
}

func (s *mockResumeStorage) ListFiles(threadIndex int, dir string) ([]string, []int64, error) {
	return nil, nil, nil
}

func (s *mockResumeStorage) DeleteFile(threadIndex int, filePath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.files, filePath)
	return nil
}

func (s *mockResumeStorage) MoveFile(threadIndex int, from string, to string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.files[to] = s.files[from]
	delete(s.files, from)
	return nil
}

func (s *mockResumeStorage) CreateDirectory(threadIndex int, dir string) error {
	return nil
}

func (s *mockResumeStorage) GetFileInfo(threadIndex int, filePath string) (bool, bool, int64, error) {
	s.mu.Lock()
	s.getFileInfoCalls++
	call := s.getFileInfoCalls
	hook := s.onGetFileInfo
	data, ok := s.files[filePath]
	s.mu.Unlock()

	if hook != nil {
		if err := hook(call, filePath); err != nil {
			return false, false, 0, err
		}
	}
	if !ok {
		return false, false, 0, nil
	}
	return true, false, int64(len(data)), nil
}

func (s *mockResumeStorage) DownloadFile(threadIndex int, filePath string, chunk *Chunk) error {
	s.mu.Lock()
	s.downloadCalls++
	call := s.downloadCalls
	hook := s.onDownload
	data, ok := s.files[filePath]
	s.mu.Unlock()

	if !ok {
		return fmt.Errorf("file not found: %s", filePath)
	}
	if hook != nil {
		return hook(call, filePath, data, chunk)
	}
	_, err := chunk.Write(data)
	return err
}

func (s *mockResumeStorage) DownloadFileRange(threadIndex int, filePath string, offset int64, chunk *Chunk) error {
	s.mu.Lock()
	s.downloadRangeCalls++
	call := s.downloadRangeCalls
	s.rangeOffsets = append(s.rangeOffsets, offset)
	hook := s.onDownloadRange
	data, ok := s.files[filePath]
	s.mu.Unlock()

	if !ok {
		return fmt.Errorf("file not found: %s", filePath)
	}
	if hook != nil {
		return hook(call, filePath, offset, data, chunk)
	}
	if offset >= int64(len(data)) {
		return ErrRangeNotSatisfiable
	}
	_, err := chunk.Write(data[offset:])
	return err
}

func (s *mockResumeStorage) UploadFile(threadIndex int, filePath string, content []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make([]byte, len(content))
	copy(copied, content)
	s.files[filePath] = copied
	return nil
}

func (s *mockResumeStorage) IsCacheNeeded() bool         { return true }
func (s *mockResumeStorage) IsMoveFileImplemented() bool { return true }
func (s *mockResumeStorage) IsStrongConsistent() bool    { return true }
func (s *mockResumeStorage) IsFastListing() bool         { return false }
func (s *mockResumeStorage) EnableTestMode()             {}

func prepareEncryptedTestChunk(t *testing.T, storage *mockResumeStorage, payload []byte, isMetadata bool) (*Config, string, string) {
	t.Helper()
	config := CreateConfig()
	config.ChunkKey = bytes.Repeat([]byte{0x42}, 32)
	config.chunkPool = make(chan *Chunk, 16)

	chunk := CreateChunk(config, true)
	chunk.Reset(true)
	chunk.isMetadata = isMetadata
	chunk.Write(payload)
	chunkHash := chunk.GetHash()
	chunkID := chunk.GetID()

	if err := chunk.Encrypt(config.ChunkKey, chunkHash, isMetadata); err != nil {
		t.Fatalf("failed to encrypt test chunk: %v", err)
	}

	chunkPath, _, _, err := storage.FindChunk(0, chunkID, false, isMetadata)
	if err != nil {
		t.Fatalf("failed to compute chunk path: %v", err)
	}
	if err := storage.UploadFile(0, chunkPath, chunk.GetBytes()); err != nil {
		t.Fatalf("failed to store test chunk: %v", err)
	}
	return config, chunkHash, chunkID
}

func withFastRetryDelay(t *testing.T) {
	t.Helper()
	orig := downloadRetryDelay
	downloadRetryDelay = time.Millisecond
	t.Cleanup(func() {
		downloadRetryDelay = orig
	})
}

func TestChunkOperatorDownloadResumeMultiSegment(t *testing.T) {
	setTestingT(t)
	withFastRetryDelay(t)

	storage := newMockResumeStorage()
	payload := bytes.Repeat([]byte("duplicacy-resumable-download-test-payload-"), 2000)
	config, chunkHash, chunkID := prepareEncryptedTestChunk(t, storage, payload, false)

	// Simulate:
	// - DownloadFile sends first 25%, then drops with io.ErrUnexpectedEOF
	// - Resume #1 sends 25%..50%, then drops with ECONNRESET
	// - Resume #2 fails immediately with 0 bytes (stalled transient error) -> must NOT reset progress
	// - Resume #3 sends 50%..75%, then drops with unexpected EOF
	// - Resume #4 sends 75%..100% and succeeds
	storage.onDownload = func(call int, filePath string, data []byte, chunk *Chunk) error {
		part := len(data) / 4
		chunk.Write(data[:part])
		return io.ErrUnexpectedEOF
	}
	storage.onDownloadRange = func(call int, filePath string, offset int64, data []byte, chunk *Chunk) error {
		q1 := int64(len(data) / 4)
		q2 := int64(len(data) / 2)
		q3 := int64(len(data) * 3 / 4)
		switch call {
		case 1:
			if offset != q1 {
				t.Errorf("resume #1 offset = %d, want %d", offset, q1)
			}
			chunk.Write(data[offset:q2])
			return syscall.ECONNRESET
		case 2:
			if offset != q2 {
				t.Errorf("resume #2 offset = %d, want %d", offset, q2)
			}
			return fmt.Errorf("i/o timeout")
		case 3:
			if offset != q2 {
				t.Errorf("resume #3 offset = %d, want %d", offset, q2)
			}
			chunk.Write(data[offset:q3])
			return fmt.Errorf("unexpected EOF")
		case 4:
			if offset != q3 {
				t.Errorf("resume #4 offset = %d, want %d", offset, q3)
			}
			chunk.Write(data[offset:])
			return nil
		default:
			return fmt.Errorf("unexpected extra resume call %d", call)
		}
	}

	operator := CreateChunkOperator(config, storage, nil, false, false, 1, true)
	defer operator.Stop()

	got := operator.Download(chunkHash, 0, false)
	if got == nil || got.isBroken {
		t.Fatalf("expected chunk download to succeed via multi-segment resume, got broken=%v", got == nil || got.isBroken)
	}
	if got.GetID() != chunkID || !bytes.Equal(got.GetBytes(), payload) {
		t.Fatalf("downloaded chunk content mismatch")
	}
	if storage.downloadCalls != 1 {
		t.Errorf("expected DownloadFile to be called once without full restart, got %d", storage.downloadCalls)
	}
	if storage.downloadRangeCalls != 4 {
		t.Errorf("expected 4 DownloadFileRange calls, got %d", storage.downloadRangeCalls)
	}
}

func TestChunkOperatorDownloadResumeRangeNotSupportedFallback(t *testing.T) {
	setTestingT(t)
	withFastRetryDelay(t)

	storage := newMockResumeStorage()
	payload := bytes.Repeat([]byte("range-not-supported-fallback-"), 1000)
	config, chunkHash, chunkID := prepareEncryptedTestChunk(t, storage, payload, false)

	storage.onDownload = func(call int, filePath string, data []byte, chunk *Chunk) error {
		if call == 1 {
			chunk.Write(data[:len(data)/3])
			return io.ErrUnexpectedEOF
		}
		chunk.Write(data)
		return nil
	}
	storage.onDownloadRange = func(call int, filePath string, offset int64, data []byte, chunk *Chunk) error {
		return ErrRangeNotSupported
	}

	operator := CreateChunkOperator(config, storage, nil, false, false, 1, true)
	defer operator.Stop()

	got := operator.Download(chunkHash, 0, false)
	if got == nil || got.isBroken {
		t.Fatalf("expected fallback full retry to succeed")
	}
	if got.GetID() != chunkID || !bytes.Equal(got.GetBytes(), payload) {
		t.Fatalf("downloaded chunk content mismatch after fallback")
	}
	if storage.downloadCalls != 2 {
		t.Errorf("expected 2 DownloadFile calls (initial + fallback), got %d", storage.downloadCalls)
	}
	if storage.downloadRangeCalls != 1 {
		t.Errorf("expected 1 DownloadFileRange call before fallback, got %d", storage.downloadRangeCalls)
	}
}

func TestChunkOperatorDownloadResume416AfterFullBytesReceived(t *testing.T) {
	setTestingT(t)
	withFastRetryDelay(t)

	storage := newMockResumeStorage()
	payload := bytes.Repeat([]byte("trailing-eof-416-test-"), 1000)
	config, chunkHash, chunkID := prepareEncryptedTestChunk(t, storage, payload, false)

	storage.onDownload = func(call int, filePath string, data []byte, chunk *Chunk) error {
		// All encrypted bytes arrived, but a trailing EOF occurred before stream termination
		chunk.Write(data)
		return io.ErrUnexpectedEOF
	}
	storage.onDownloadRange = func(call int, filePath string, offset int64, data []byte, chunk *Chunk) error {
		if offset != int64(len(data)) {
			t.Errorf("expected resume offset %d, got %d", len(data), offset)
		}
		return ErrRangeNotSatisfiable
	}

	operator := CreateChunkOperator(config, storage, nil, false, false, 1, true)
	defer operator.Stop()

	got := operator.Download(chunkHash, 0, false)
	if got == nil || got.isBroken {
		t.Fatalf("expected chunk to verify and succeed after 416 without restarting")
	}
	if got.GetID() != chunkID || !bytes.Equal(got.GetBytes(), payload) {
		t.Fatalf("downloaded chunk content mismatch")
	}
	if storage.downloadCalls != 1 {
		t.Errorf("expected only 1 DownloadFile call, got %d", storage.downloadCalls)
	}
	if storage.downloadRangeCalls != 1 {
		t.Errorf("expected 1 DownloadFileRange call, got %d", storage.downloadRangeCalls)
	}
}

func TestChunkOperatorFindChunkTransientRetry(t *testing.T) {
	setTestingT(t)
	withFastRetryDelay(t)

	storage := newMockResumeStorage()
	payload := []byte("find-chunk-transient-retry-content")
	config, chunkHash, chunkID := prepareEncryptedTestChunk(t, storage, payload, false)

	// Reset call counter after prepareEncryptedTestChunk
	storage.getFileInfoCalls = 0
	storage.onGetFileInfo = func(call int, filePath string) error {
		if call <= 2 {
			return syscall.ECONNRESET
		}
		return nil
	}

	operator := CreateChunkOperator(config, storage, nil, false, false, 1, true)
	defer operator.Stop()

	got := operator.Download(chunkHash, 0, false)
	if got == nil || got.isBroken {
		t.Fatalf("expected FindChunk to retry transient network error and succeed")
	}
	if got.GetID() != chunkID || !bytes.Equal(got.GetBytes(), payload) {
		t.Fatalf("downloaded chunk mismatch")
	}
	if storage.getFileInfoCalls != 3 {
		t.Errorf("expected 3 GetFileInfo calls (2 transient failures + 1 success), got %d", storage.getFileInfoCalls)
	}
}

func TestChunkOperatorDataChunkCache(t *testing.T) {
	setTestingT(t)
	withFastRetryDelay(t)

	storage := newMockResumeStorage()
	dataPayload := bytes.Repeat([]byte("cached-data-chunk-"), 256)
	config, dataHash, dataID := prepareEncryptedTestChunk(t, storage, dataPayload, false)

	metaPayload := bytes.Repeat([]byte("cached-meta-chunk-"), 128)
	metaChunk := CreateChunk(config, true)
	metaChunk.Reset(true)
	metaChunk.isMetadata = true
	metaChunk.Write(metaPayload)
	metaHash := metaChunk.GetHash()
	metaID := metaChunk.GetID()
	if err := metaChunk.Encrypt(config.ChunkKey, metaHash, true); err != nil {
		t.Fatalf("failed to encrypt metadata chunk: %v", err)
	}
	metaPath, _, _, err := storage.FindChunk(0, metaID, false, true)
	if err != nil {
		t.Fatalf("failed to compute metadata path: %v", err)
	}
	storage.UploadFile(0, metaPath, metaChunk.GetBytes())

	cacheDir := t.TempDir()
	snapshotCache, err := CreateFileStorage(cacheDir, false, 1)
	if err != nil {
		t.Fatalf("failed to create snapshot cache: %v", err)
	}
	snapshotCache.SetDefaultNestingLevels([]int{1}, 1)
	if err := snapshotCache.CreateDirectory(0, "chunks"); err != nil {
		t.Fatalf("failed to create chunks cache dir: %v", err)
	}

	operator := CreateChunkOperator(config, storage, snapshotCache, false, false, 1, true)
	operator.EnableDataChunkCache()
	defer operator.Stop()

	// 1. First download populates local cache for both data and metadata chunks.
	got1 := operator.Download(dataHash, 0, false)
	if got1 == nil || got1.isBroken || !bytes.Equal(got1.GetBytes(), dataPayload) {
		t.Fatalf("initial data chunk download failed")
	}
	gotMeta1 := operator.Download(metaHash, 0, true)
	if gotMeta1 == nil || gotMeta1.isBroken || !bytes.Equal(gotMeta1.GetBytes(), metaPayload) {
		t.Fatalf("initial metadata chunk download failed")
	}
	if storage.downloadCalls != 2 {
		t.Fatalf("expected 2 remote downloads, got %d", storage.downloadCalls)
	}

	// Reset progress counter to check cache-hit accounting cleanly.
	atomic.StoreInt64(&operator.downloadedChunkSize, 0)

	// 2. Metadata cache hit must NOT inflate downloadedChunkSize.
	gotMeta2 := operator.Download(metaHash, 0, true)
	if gotMeta2 == nil || gotMeta2.isBroken || !bytes.Equal(gotMeta2.GetBytes(), metaPayload) {
		t.Fatalf("cached metadata chunk load failed")
	}
	if atomic.LoadInt64(&operator.downloadedChunkSize) != 0 {
		t.Errorf("expected downloadedChunkSize=0 after cached metadata hit, got %d", atomic.LoadInt64(&operator.downloadedChunkSize))
	}

	// 3. Data chunk cache hit should NOT hit remote storage and MUST advance downloadedChunkSize.
	got2 := operator.Download(dataHash, 0, false)
	if got2 == nil || got2.isBroken || !bytes.Equal(got2.GetBytes(), dataPayload) {
		t.Fatalf("cached data chunk load failed")
	}
	if storage.downloadCalls != 2 {
		t.Errorf("expected no new remote downloads on cache hit, got %d", storage.downloadCalls)
	}
	if atomic.LoadInt64(&operator.downloadedChunkSize) != int64(len(dataPayload)) {
		t.Errorf("expected downloadedChunkSize=%d, got %d", len(dataPayload), atomic.LoadInt64(&operator.downloadedChunkSize))
	}

	// 4. Corrupted cache file should be discarded and transparently re-downloaded from storage.
	cachedPath, exist, _, err := snapshotCache.FindChunk(0, dataID, false, false)
	if err != nil || !exist {
		t.Fatalf("expected data chunk to exist in cache (exist=%v, err=%v)", exist, err)
	}
	if err := os.WriteFile(path.Join(cacheDir, cachedPath), []byte("corrupted-cache-bytes"), 0644); err != nil {
		t.Fatalf("failed to corrupt cache file: %v", err)
	}

	got3 := operator.Download(dataHash, 0, false)
	if got3 == nil || got3.isBroken || !bytes.Equal(got3.GetBytes(), dataPayload) {
		t.Fatalf("expected recovery from corrupted cache file")
	}
	if storage.downloadCalls != 3 {
		t.Errorf("expected remote re-download after cache corruption, got %d calls", storage.downloadCalls)
	}
}

func TestFileStorageDownloadRange(t *testing.T) {
	dir := t.TempDir()
	storage, err := CreateFileStorage(dir, false, 1)
	if err != nil {
		t.Fatalf("CreateFileStorage failed: %v", err)
	}

	content := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	if err := storage.UploadFile(0, "testchunk", content); err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	config := CreateConfig()
	chunk := CreateChunk(config, true)
	chunk.Reset(false)
	chunk.Write(content[:10])

	if err := storage.DownloadFileRange(0, "testchunk", 10, chunk); err != nil {
		t.Fatalf("DownloadFileRange failed: %v", err)
	}
	if !bytes.Equal(chunk.GetBytes(), content) {
		t.Fatalf("got %q, want %q", chunk.GetBytes(), content)
	}
}

func TestWebDAVStorageDownloadRangeAndRedirects(t *testing.T) {
	setTestingT(t)

	content := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	ignoreRange := false

	var serverURL string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "PROPFIND":
			if r.URL.Path == "/root/dir301" {
				w.Header().Set("Location", "/root/dir301/")
				w.WriteHeader(http.StatusMovedPermanently)
				return
			}
			w.WriteHeader(http.StatusMultiStatus)
			fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><D:multistatus xmlns:D="DAV:"><D:response><D:href>%s</D:href><D:propstat><D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop></D:propstat></D:response></D:multistatus>`, r.URL.Path)
		case "MKCOL":
			w.WriteHeader(http.StatusCreated)
		case "GET":
			if r.URL.Path == "/root/redirect302" {
				http.Redirect(w, r, serverURL+"/root/file", http.StatusFound)
				return
			}
			if r.URL.Path == "/root/file" {
				rangeHdr := r.Header.Get("Range")
				if rangeHdr != "" && !ignoreRange {
					offsetStr := strings.TrimSuffix(strings.TrimPrefix(rangeHdr, "bytes="), "-")
					offset, err := strconv.Atoi(offsetStr)
					if err != nil || offset < 0 || offset >= len(content) {
						w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
						return
					}
					w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, len(content)-1, len(content)))
					w.WriteHeader(http.StatusPartialContent)
					w.Write(content[offset:])
					return
				}
				w.WriteHeader(http.StatusOK)
				w.Write(content)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()
	serverURL = ts.URL

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	port, _ := strconv.Atoi(u.Port())
	storage, err := CreateWebDAVStorage(u.Hostname(), port, "user", "pass", "root", true, 1)
	if err != nil {
		t.Fatalf("CreateWebDAVStorage failed: %v", err)
	}

	// 1. 301 Moved Permanently must NOT be followed so directory detection keeps working.
	exist, isDir, _, err := storage.GetFileInfo(0, "dir301")
	if err != nil || !exist || !isDir {
		t.Fatalf("expected 301 directory detection (exist=true, isDir=true, err=nil), got exist=%v isDir=%v err=%v", exist, isDir, err)
	}

	// 2. 302 redirect (used by aliyunpan) must still be followed for GET and ranged GET.
	config := CreateConfig()
	chunk := CreateChunk(config, true)
	chunk.Reset(false)
	chunk.Write(content[:10])
	if err := storage.DownloadFileRange(0, "redirect302", 10, chunk); err != nil {
		t.Fatalf("DownloadFileRange across 302 failed: %v", err)
	}
	if !bytes.Equal(chunk.GetBytes(), content) {
		t.Fatalf("ranged download across 302 got %q, want %q", chunk.GetBytes(), content)
	}

	// 3. HTTP 416 must return ErrRangeNotSatisfiable immediately.
	chunk.Reset(false)
	err = storage.DownloadFileRange(0, "file", int64(len(content)), chunk)
	if !errors.Is(err, ErrRangeNotSatisfiable) {
		t.Fatalf("expected ErrRangeNotSatisfiable on 416, got %v", err)
	}

	// 4. Server returning 200 OK instead of 206 must return ErrRangeNotSupported without appending.
	ignoreRange = true
	chunk.Reset(false)
	chunk.Write(content[:10])
	err = storage.DownloadFileRange(0, "file", 10, chunk)
	if !errors.Is(err, ErrRangeNotSupported) {
		t.Fatalf("expected ErrRangeNotSupported when server returns 200, got %v", err)
	}
	if !bytes.Equal(chunk.GetBytes(), content[:10]) {
		t.Fatalf("chunk was modified when server ignored Range: got %q", chunk.GetBytes())
	}

	// 5. Verify aliyundrive 302 CheckRedirect header rewriting is preserved.
	req, _ := http.NewRequest("GET", "https://cn-beijing-data.aliyundrive.net/file", nil)
	req.Header.Set("Authorization", "Basic secret")
	prev, _ := http.NewRequest("GET", "http://example.com/dav/file", nil)
	req.Response = &http.Response{StatusCode: http.StatusFound}
	if err := storage.client.CheckRedirect(req, []*http.Request{prev}); err != nil {
		t.Fatalf("expected CheckRedirect to follow 302 for aliyundrive, got %v", err)
	}
	if req.Header.Get("Referer") != "https://www.aliyundrive.com/" {
		t.Errorf("expected aliyundrive Referer header, got %q", req.Header.Get("Referer"))
	}
	if req.Header.Get("Authorization") != "" {
		t.Errorf("expected Authorization header to be removed for aliyundrive, got %q", req.Header.Get("Authorization"))
	}
}

func TestB2ClientDownloadRange(t *testing.T) {
	setTestingT(t)

	content := []byte("backblaze-b2-range-content")
	var lastRangeHeader string
	mode := "normal"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastRangeHeader = r.Header.Get("Range")
		if strings.HasSuffix(r.URL.Path, "/empty") {
			if lastRangeHeader != "" {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		switch mode {
		case "ignore-range":
			w.WriteHeader(http.StatusOK)
			w.Write(content)
		case "416":
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		default:
			if lastRangeHeader != "" {
				offsetStr := strings.TrimSuffix(strings.TrimPrefix(lastRangeHeader, "bytes="), "-")
				offset, _ := strconv.Atoi(offsetStr)
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, len(content)-1, len(content)))
				w.WriteHeader(http.StatusPartialContent)
				w.Write(content[offset:])
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write(content)
		}
	}))
	defer ts.Close()

	client := NewB2Client("id", "key", ts.URL, "", 1)
	client.HTTPClient = ts.Client()
	client.APIURL = ts.URL
	client.BucketName = "bucket"
	client.AuthorizationToken = "token"
	client.IsAuthorized = true

	// 1. DownloadFile on empty file must NOT send Range header (avoids 416).
	rc, _, err := client.DownloadFile(0, "empty")
	if err != nil {
		t.Fatalf("DownloadFile on empty file failed: %v", err)
	}
	rc.Close()
	if lastRangeHeader != "" {
		t.Errorf("expected no Range header for DownloadFile, got %q", lastRangeHeader)
	}

	// 2. Ranged download (206) succeeds.
	rc, _, err = client.DownloadFileRange(0, "chunk1", 10)
	if err != nil {
		t.Fatalf("DownloadFileRange failed: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, content[10:]) {
		t.Fatalf("got %q, want %q", got, content[10:])
	}

	// 3. Server returning 200 OK instead of 206 -> ErrRangeNotSupported.
	mode = "ignore-range"
	_, _, err = client.DownloadFileRange(0, "chunk1", 10)
	if !errors.Is(err, ErrRangeNotSupported) {
		t.Fatalf("expected ErrRangeNotSupported, got %v", err)
	}

	// 4. Server returning 416 -> ErrRangeNotSatisfiable immediately.
	mode = "416"
	_, _, err = client.DownloadFileRange(0, "chunk1", int64(len(content)))
	if !errors.Is(err, ErrRangeNotSatisfiable) {
		t.Fatalf("expected ErrRangeNotSatisfiable, got %v", err)
	}
}

func TestOneDriveClientDownloadRange(t *testing.T) {
	setTestingT(t)

	content := []byte("onedrive-range-content-test")
	mode := "normal"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHdr := r.Header.Get("Range")
		switch mode {
		case "ignore-range":
			w.WriteHeader(http.StatusOK)
			w.Write(content)
		case "416":
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		default:
			if rangeHdr != "" {
				offsetStr := strings.TrimSuffix(strings.TrimPrefix(rangeHdr, "bytes="), "-")
				offset, _ := strconv.Atoi(offsetStr)
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, len(content)-1, len(content)))
				w.WriteHeader(http.StatusPartialContent)
				w.Write(content[offset:])
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write(content)
		}
	}))
	defer ts.Close()

	client := &OneDriveClient{
		HTTPClient: ts.Client(),
		Token:      &oauth2.Token{AccessToken: "test-token"},
		TokenLock:  &sync.Mutex{},
		APIURL:     ts.URL,
	}

	// 1. 206 Partial Content succeeds.
	rc, _, err := client.DownloadFileRange("chunks/abc", 9)
	if err != nil {
		t.Fatalf("DownloadFileRange failed: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, content[9:]) {
		t.Fatalf("got %q, want %q", got, content[9:])
	}

	// 2. 200 OK instead of 206 -> ErrRangeNotSupported.
	mode = "ignore-range"
	_, _, err = client.DownloadFileRange("chunks/abc", 9)
	if !errors.Is(err, ErrRangeNotSupported) {
		t.Fatalf("expected ErrRangeNotSupported, got %v", err)
	}

	// 3. 416 -> ErrRangeNotSatisfiable immediately (no 12x retry loop).
	mode = "416"
	_, _, err = client.DownloadFileRange("chunks/abc", int64(len(content)))
	if !errors.Is(err, ErrRangeNotSatisfiable) {
		t.Fatalf("expected ErrRangeNotSatisfiable, got %v", err)
	}
}

type capturedLogEntry struct {
	level   int
	logID   string
	message string
}

func captureLogs(t *testing.T, fn func()) []capturedLogEntry {
	t.Helper()
	var mu sync.Mutex
	var entries []capturedLogEntry
	origLogFunc := LogFunction
	origLevel := loggingLevel
	SetLoggingLevel(INFO)
	LogFunction = func(level int, logID string, message string) {
		mu.Lock()
		entries = append(entries, capturedLogEntry{level: level, logID: logID, message: message})
		mu.Unlock()
		if level > WARN {
			panic(Exception{Level: level, LogID: logID, Message: message})
		}
	}
	defer func() {
		LogFunction = origLogFunc
		SetLoggingLevel(origLevel)
	}()
	fn()
	return entries
}

func writeDeterministicFile(t *testing.T, filePath string, size int, seed uint64) {
	t.Helper()
	data := make([]byte, size)
	state := seed*6364136223846793005 + 1442695040888963407
	for i := range data {
		state = state*6364136223846793005 + 1442695040888963407
		data[i] = byte(state >> 56)
	}
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		t.Fatalf("failed to write %s: %v", filePath, err)
	}
}

func setupRestoreResumeTestEnv(t *testing.T, files []struct {
	name string
	size int
}, fixedChunkSize bool) (string, string, string, *FileStorage) {
	t.Helper()
	baseDir := t.TempDir()
	srcRepo := path.Join(baseDir, "src_repo")
	dstRepo := path.Join(baseDir, "dst_repo")
	storageDir := path.Join(baseDir, "storage")
	os.MkdirAll(path.Join(srcRepo, ".duplicacy"), 0700)
	os.MkdirAll(path.Join(dstRepo, ".duplicacy"), 0700)
	os.MkdirAll(storageDir, 0700)

	for idx, f := range files {
		full := path.Join(srcRepo, f.name)
		os.MkdirAll(path.Dir(full), 0700)
		writeDeterministicFile(t, full, f.size, uint64(idx+1))
	}

	storage, err := CreateFileStorage(storageDir, false, 1)
	if err != nil {
		t.Fatalf("CreateFileStorage failed: %v", err)
	}
	for _, dir := range []string{"chunks", "snapshots"} {
		if err := storage.CreateDirectory(0, dir); err != nil {
			t.Fatalf("CreateDirectory %s failed: %v", dir, err)
		}
	}
	if fixedChunkSize {
		if !ConfigStorage(storage, 16384, 100, 64*1024, 64*1024, 64*1024, "", nil, false, "", 0, 0) {
			t.Fatalf("ConfigStorage (fixed) failed")
		}
	} else {
		if !ConfigStorage(storage, 16384, 100, 64*1024, 256*1024, 16*1024, "", nil, false, "", 0, 0) {
			t.Fatalf("ConfigStorage (variable) failed")
		}
	}

	SetDuplicacyPreferencePath(path.Join(srcRepo, ".duplicacy"))
	bm := CreateBackupManager("resume-test", storage, srcRepo, "", "", "", false)
	bm.SetupSnapshotCache("default")
	if !bm.Backup(srcRepo, true, 1, "init", false, false, 0, false, 1024, 1024) {
		t.Fatalf("Backup failed")
	}

	return srcRepo, dstRepo, storageDir, storage
}

var (
	reDownloadedProgress = regexp.MustCompile(`^Downloaded chunk (\d+)/(\d+) size (\d+), .* ([0-9.]+)%$`)
	reCachedProgress     = regexp.MustCompile(`^Cached chunk (\d+)/(\d+) size (\d+), ([0-9.]+)%$`)
)

func TestRestoreResumeProgressWithSkippedFilesAndChunkCache(t *testing.T) {
	setTestingT(t)

	// Variable chunking packs file1, file2, file3 continuously so file1 and file2 share a boundary chunk.
	files := []struct {
		name string
		size int
	}{
		{"file1", 400 * 1024},
		{"file2", 400 * 1024},
		{"file3", 400 * 1024},
	}
	srcRepo, dstRepo, _, storage := setupRestoreResumeTestEnv(t, files, false)

	SetDuplicacyPreferencePath(path.Join(dstRepo, ".duplicacy"))
	bm := CreateBackupManager("resume-test", storage, dstRepo, "", "", "", false)
	bm.SetupSnapshotCache("default")
	bm.SetDataChunkCaching(true)

	// First run: restore everything to populate files and local chunk cache.
	firstLogs := captureLogs(t, func() {
		failed := bm.Restore(dstRepo, 1, true, true, 1, true, false, false, true, nil, false)
		if failed != 0 {
			t.Fatalf("initial restore failed with %d errors", failed)
		}
	})

	totalChunks := 0
	for _, entry := range firstLogs {
		if entry.logID == "DOWNLOAD_PROGRESS" {
			if m := reDownloadedProgress.FindStringSubmatch(entry.message); m != nil {
				totalChunks, _ = strconv.Atoi(m[2])
			}
		}
	}
	if totalChunks < 10 {
		t.Fatalf("expected >= 10 total chunks in snapshot, got %d", totalChunks)
	}

	// Simulate an interrupted restore right after file1 finished:
	// - file1 stays intact on disk (will be skipped by quickMode)
	// - file2 and file3 are removed from disk
	// - keep ONLY file1's chunks (including the boundary chunk shared with file2) in the local cache,
	//   deleting later data chunks from the local cache so file2's shared boundary chunk hits cache
	//   and the following chunks must be downloaded from remote storage.
	// Previously this caused file2 to be re-indexed starting from 0, silently loading the shared
	// chunk (index 0) from cache and logging "Downloaded chunk 2 ... 2.x%".
	os.Remove(path.Join(dstRepo, "file2"))
	os.Remove(path.Join(dstRepo, "file3"))

	snap := bm.SnapshotManager.DownloadSnapshot("resume-test", 1)
	bm.SnapshotManager.DownloadSnapshotSequences(snap)
	var file1EndChunk, file2StartChunk int
	metaOp := CreateChunkOperator(bm.config, storage, bm.snapshotCache, false, false, 1, false)
	snap.ListRemoteFiles(bm.config, metaOp, func(e *Entry) bool {
		if e.Path == "file1" {
			file1EndChunk = e.EndChunk
		} else if e.Path == "file2" {
			file2StartChunk = e.StartChunk
		}
		return true
	})
	metaOp.Stop()

	if file1EndChunk != file2StartChunk {
		t.Fatalf("expected file1 and file2 to share a boundary chunk, got file1.EndChunk=%d file2.StartChunk=%d",
			file1EndChunk, file2StartChunk)
	}
	for i := file1EndChunk + 1; i < len(snap.ChunkHashes); i++ {
		chunkID := bm.config.GetChunkIDFromHash(snap.ChunkHashes[i])
		if cachedPath, exist, _, err := bm.snapshotCache.FindChunk(0, chunkID, false, false); err == nil && exist {
			bm.snapshotCache.DeleteFile(0, cachedPath)
		}
	}

	// Second run: restart restore from breakpoint.
	resumeLogs := captureLogs(t, func() {
		failed := bm.Restore(dstRepo, 1, true, true, 1, true, false, false, true, nil, false)
		if failed != 0 {
			t.Fatalf("resumed restore failed with %d errors", failed)
		}
	})

	sawResumeLog := false
	firstCachedIdx := -1
	firstDownloadedIdx := -1
	firstDownloadedPct := 0.0
	lastDownloadedIdx := -1
	lastDownloadedPct := 0.0

	for _, entry := range resumeLogs {
		if entry.logID == "RESTORE_RESUME" {
			sawResumeLog = true
		}
		if entry.logID == "DOWNLOAD_PROGRESS" {
			if m := reCachedProgress.FindStringSubmatch(entry.message); m != nil {
				idx, _ := strconv.Atoi(m[1])
				tot, _ := strconv.Atoi(m[2])
				if tot != totalChunks {
					t.Errorf("cached progress totalChunks = %d, want %d", tot, totalChunks)
				}
				if firstCachedIdx == -1 {
					firstCachedIdx = idx
				}
			}
			if m := reDownloadedProgress.FindStringSubmatch(entry.message); m != nil {
				idx, _ := strconv.Atoi(m[1])
				tot, _ := strconv.Atoi(m[2])
				pct, _ := strconv.ParseFloat(m[4], 64)
				if tot != totalChunks {
					t.Errorf("download progress totalChunks = %d, want %d", tot, totalChunks)
				}
				if firstDownloadedIdx == -1 {
					firstDownloadedIdx = idx
					firstDownloadedPct = pct
				}
				lastDownloadedIdx = idx
				lastDownloadedPct = pct
			}
		}
	}

	if !sawResumeLog {
		t.Errorf("expected RESTORE_RESUME log message when resuming with skipped files")
	}
	expectedSharedChunk := file1EndChunk + 1 // 1-based chunk index of shared boundary chunk
	if firstCachedIdx != expectedSharedChunk {
		t.Errorf("expected first cached chunk to be %d/%d, got %d", expectedSharedChunk, totalChunks, firstCachedIdx)
	}
	expectedFirstDownload := expectedSharedChunk + 1
	if firstDownloadedIdx != expectedFirstDownload {
		t.Errorf("expected first downloaded chunk to be %d/%d (not 2!), got %d", expectedFirstDownload, totalChunks, firstDownloadedIdx)
	}
	// file1 is ~33% of total size plus 1 new chunk -> percentage must reflect true progress (> 33%), not ~5%.
	if firstDownloadedPct < 33.0 {
		t.Errorf("expected first downloaded chunk percentage >= 33%%, got %.1f%%", firstDownloadedPct)
	}
	if lastDownloadedIdx != totalChunks || lastDownloadedPct != 100.0 {
		t.Errorf("expected final progress %d/%d at 100.0%%, got %d/%d at %.1f%%", totalChunks, totalChunks, lastDownloadedIdx, totalChunks, lastDownloadedPct)
	}

	for _, f := range files {
		if getFileHash(path.Join(srcRepo, f.name)) != getFileHash(path.Join(dstRepo, f.name)) {
			t.Errorf("restored file %s hash mismatch", f.name)
		}
	}
}

func TestRestoreResumeProgressInPlacePartialFile(t *testing.T) {
	setTestingT(t)

	// Single 640KB file -> 10 fixed 64KB chunks.
	files := []struct {
		name string
		size int
	}{
		{"bigfile", 10 * 64 * 1024},
	}
	srcRepo, dstRepo, _, storage := setupRestoreResumeTestEnv(t, files, true)

	SetDuplicacyPreferencePath(path.Join(dstRepo, ".duplicacy"))
	bm := CreateBackupManager("resume-test", storage, dstRepo, "", "", "", false)
	bm.SetupSnapshotCache("default")
	// Chunk cache disabled: test pure in-place local disk resume.
	bm.SetDataChunkCaching(false)

	// Pre-populate dstRepo/bigfile with the first 4 chunks (256KB) to simulate an interrupted restore.
	srcBytes, err := os.ReadFile(path.Join(srcRepo, "bigfile"))
	if err != nil {
		t.Fatalf("read src bigfile: %v", err)
	}
	if err := os.WriteFile(path.Join(dstRepo, "bigfile"), srcBytes[:4*64*1024], 0644); err != nil {
		t.Fatalf("write partial bigfile: %v", err)
	}

	logs := captureLogs(t, func() {
		failed := bm.Restore(dstRepo, 1, true, true, 1, true, false, false, true, nil, false)
		if failed != 0 {
			t.Fatalf("in-place resumed restore failed with %d errors", failed)
		}
	})

	sawResumeLog := false
	firstIdx := -1
	firstPct := 0.0
	lastIdx := -1
	lastPct := 0.0

	for _, entry := range logs {
		if entry.logID == "RESTORE_RESUME" && strings.Contains(entry.message, "4/10 chunks") {
			sawResumeLog = true
		}
		if entry.logID == "DOWNLOAD_PROGRESS" {
			if m := reDownloadedProgress.FindStringSubmatch(entry.message); m != nil {
				idx, _ := strconv.Atoi(m[1])
				tot, _ := strconv.Atoi(m[2])
				pct, _ := strconv.ParseFloat(m[4], 64)
				if tot != 10 {
					t.Errorf("totalChunks = %d, want 10", tot)
				}
				if firstIdx == -1 {
					firstIdx = idx
					firstPct = pct
				}
				lastIdx = idx
				lastPct = pct
			}
		}
	}

	if !sawResumeLog {
		t.Errorf("expected RESTORE_RESUME log mentioning 4/10 chunks already exist locally")
	}
	if firstIdx != 5 || firstPct != 50.0 {
		t.Errorf("expected first downloaded chunk 5/10 at 50.0%%, got %d/10 at %.1f%%", firstIdx, firstPct)
	}
	if lastIdx != 10 || lastPct != 100.0 {
		t.Errorf("expected final downloaded chunk 10/10 at 100.0%%, got %d/10 at %.1f%%", lastIdx, lastPct)
	}
	if getFileHash(path.Join(srcRepo, "bigfile")) != getFileHash(path.Join(dstRepo, "bigfile")) {
		t.Errorf("restored bigfile hash mismatch")
	}
}

func TestRestoreResumeProgressHashModeSkippedFile(t *testing.T) {
	setTestingT(t)

	// Two chunk-aligned 320KB files (5 chunks each = 10 chunks total).
	files := []struct {
		name string
		size int
	}{
		{"a_file1", 5 * 64 * 1024},
		{"b_file2", 5 * 64 * 1024},
	}
	srcRepo, dstRepo, _, storage := setupRestoreResumeTestEnv(t, files, true)

	SetDuplicacyPreferencePath(path.Join(dstRepo, ".duplicacy"))
	bm := CreateBackupManager("resume-test", storage, dstRepo, "", "", "", false)
	bm.SetupSnapshotCache("default")
	bm.SetDataChunkCaching(false)

	// Restore full snapshot once, then delete b_file2 while keeping a_file1.
	if failed := bm.Restore(dstRepo, 1, true, true, 1, true, false, false, false, nil, false); failed != 0 {
		t.Fatalf("initial restore failed: %d", failed)
	}
	os.Remove(path.Join(dstRepo, "b_file2"))

	// Resume in -hash mode (quickMode=false): a_file1 is skipped by hash inside RestoreFile.
	logs := captureLogs(t, func() {
		failed := bm.Restore(dstRepo, 1, true, false, 1, true, false, false, true, nil, false)
		if failed != 0 {
			t.Fatalf("hash-mode resumed restore failed: %d", failed)
		}
	})

	firstIdx := -1
	firstPct := 0.0
	lastIdx := -1
	lastPct := 0.0
	for _, entry := range logs {
		if entry.logID == "DOWNLOAD_PROGRESS" {
			if m := reDownloadedProgress.FindStringSubmatch(entry.message); m != nil {
				idx, _ := strconv.Atoi(m[1])
				tot, _ := strconv.Atoi(m[2])
				pct, _ := strconv.ParseFloat(m[4], 64)
				if tot != 10 {
					t.Errorf("totalChunks = %d, want 10", tot)
				}
				if firstIdx == -1 {
					firstIdx = idx
					firstPct = pct
				}
				lastIdx = idx
				lastPct = pct
			}
		}
	}

	if firstIdx != 6 || firstPct != 60.0 {
		t.Errorf("expected first downloaded chunk 6/10 at 60.0%%, got %d/10 at %.1f%%", firstIdx, firstPct)
	}
	if lastIdx != 10 || lastPct != 100.0 {
		t.Errorf("expected final downloaded chunk 10/10 at 100.0%%, got %d/10 at %.1f%%", lastIdx, lastPct)
	}
	for _, f := range files {
		if getFileHash(path.Join(srcRepo, f.name)) != getFileHash(path.Join(dstRepo, f.name)) {
			t.Errorf("restored file %s hash mismatch", f.name)
		}
	}
}

func TestRestoreResumeMultiThreadTrailingSkippedFilesNoDeadlock(t *testing.T) {
	setTestingT(t)

	// file1 is chunk-aligned or variable; file2 and file3 share a boundary chunk so AddFiles
	// marks that shared chunk as needed=true. Deleting only file1 leaves trailing skipped files
	// whose shared chunk must not trigger a deadlock under threads > 1 in either quickMode or -hash mode.
	files := []struct {
		name string
		size int
	}{
		{"a_file1", 200 * 1024},
		{"b_file2", 30 * 1024},
		{"c_file3", 200 * 1024},
	}
	srcRepo, dstRepo, _, storage := setupRestoreResumeTestEnv(t, files, false)

	SetDuplicacyPreferencePath(path.Join(dstRepo, ".duplicacy"))
	bm := CreateBackupManager("resume-test", storage, dstRepo, "", "", "", false)
	bm.SetupSnapshotCache("default")
	bm.SetDataChunkCaching(true)

	// Initial full restore
	if failed := bm.Restore(dstRepo, 1, true, true, 4, true, false, false, false, nil, false); failed != 0 {
		t.Fatalf("initial restore failed: %d", failed)
	}

	// Case 1: quickMode=true with threads=4, only a_file1 deleted (trailing b_file2 & c_file3 skipped).
	os.Remove(path.Join(dstRepo, "a_file1"))
	done := make(chan int, 1)
	go func() {
		done <- bm.Restore(dstRepo, 1, true, true, 4, true, false, false, true, nil, false)
	}()
	select {
	case failed := <-done:
		if failed != 0 {
			t.Fatalf("quickMode multi-thread resume failed: %d", failed)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("deadlock detected in quickMode multi-thread restore with trailing skipped files")
	}

	// Case 2: quickMode=false (-hash) with threads=4, only a_file1 deleted.
	os.Remove(path.Join(dstRepo, "a_file1"))
	done2 := make(chan int, 1)
	go func() {
		done2 <- bm.Restore(dstRepo, 1, true, false, 4, true, false, false, true, nil, false)
	}()
	select {
	case failed := <-done2:
		if failed != 0 {
			t.Fatalf("hashMode multi-thread resume failed: %d", failed)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("deadlock detected in hashMode multi-thread restore with trailing skipped files")
	}

	// Verify all files (including skipped ones) are present and match original hashes.
	for _, f := range files {
		if getFileHash(path.Join(srcRepo, f.name)) != getFileHash(path.Join(dstRepo, f.name)) {
			t.Errorf("restored file %s hash mismatch", f.name)
		}
	}
}
