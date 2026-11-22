// Copyright (c) Acrosync LLC. All rights reserved.
// Free for personal use and commercial trial
// Commercial use requires per-user licenses available from https://duplicacy.com

package duplicacy

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// downloadRetryDelay is the base unit of backoff between download retries and resumes.
// Tests may override this to avoid real sleeps.
var downloadRetryDelay = time.Second

// These are operations that ChunkOperator will perform.
const (
	ChunkOperationDownload  = 0
	ChunkOperationUpload    = 1
	ChunkOperationDelete    = 2
	ChunkOperationFossilize = 3
	ChunkOperationResurrect = 4
	ChunkOperationFind = 5
)

// ChunkTask is used to pass parameters for different kinds of chunk operations.
type ChunkTask struct {
	operation  int    // The type of operation
	chunkID    string // The chunk id
	chunkHash  string // The chunk hash
	chunkIndex int    // The chunk index
	filePath   string // The path of the chunk file; it may be empty

	isMetadata bool // The chunk is a metadata chunk rather than a file chunk
	skipCache  bool // Bypass the snapshot cache, i.e., read the chunk from the storage itself
	chunk      *Chunk

	completionFunc func(chunk *Chunk, chunkIndex int)
}

// ChunkOperator is capable of performing multi-threaded operations on chunks.
type ChunkOperator struct {
	config *Config               // Associated config
	storage Storage              // This storage
	snapshotCache  *FileStorage
	showStatistics bool
	threads  int                 // Number of threads
	taskQueue chan ChunkTask     // Operating goroutines are waiting on this channel for input
	stopChannel chan bool        // Used to stop all the goroutines

	numberOfActiveTasks int64    // The number of chunks that are being operated on

	fossils []string             // For fossilize operation, the paths of the fossils are stored in this slice
	collectionLock *sync.Mutex   // The lock for accessing 'fossils'

	startTime                 int64 // The time it starts downloading
	remoteDownloadStartTime   int64 // The time the first remote data chunk download starts
	totalChunkSize            int64 // Total chunk size
	totalChunks               int   // Total number of chunks in the restore task list
	downloadedChunkSize       int64 // Completed chunk size (skipped + cached + remotely downloaded)
	remoteDownloadedChunkSize int64 // Chunk size actually downloaded from remote storage
	cachedChunkCount          int64 // Number of data chunks loaded from the local cache

	allowFailures bool           // Whether to fail on download error, or continue
	cacheDataChunks bool         // Whether to cache downloaded data chunks in the snapshot cache
	NumberOfFailedChunks int64   // The number of chunks that can't be downloaded

	rewriteChunks bool           // Whether to rewrite corrupted chunks when erasure coding is enabled

	UploadCompletionFunc func(chunk *Chunk, chunkIndex int, inCache bool, chunkSize int, uploadSize int)
}

// CreateChunkOperator creates a new ChunkOperator.
func CreateChunkOperator(config *Config, storage Storage, snapshotCache *FileStorage, showStatistics bool, rewriteChunks bool, threads int,
						 allowFailures bool) *ChunkOperator {

	operator := &ChunkOperator{
		config: config,
		storage: storage,
		snapshotCache: snapshotCache,
		showStatistics: showStatistics,
		threads: threads,

		taskQueue:   make(chan ChunkTask, threads),
		stopChannel: make(chan bool),

		collectionLock: &sync.Mutex{},
		startTime: time.Now().Unix(),
		allowFailures: allowFailures,
		rewriteChunks: rewriteChunks,
	}

	// Start the operator goroutines
	for i := 0; i < operator.threads; i++ {
		go func(threadIndex int) {
			defer CatchLogException()
			for {
				select {
				case task := <-operator.taskQueue:
					operator.Run(threadIndex, task)
				case <-operator.stopChannel:
					return
				}
			}
		}(i)
	}

	return operator
}

// EnableDataChunkCache enables caching of downloaded data chunks in the local snapshot cache, so
// that subsequent runs can load them locally instead of downloading them from the storage again.
// Note that the cached chunks are stored unencrypted.
func (operator *ChunkOperator) EnableDataChunkCache() {
	operator.cacheDataChunks = true
}

// ResetDownloadProgress initializes total chunk progress counters for a restore operation.
func (operator *ChunkOperator) ResetDownloadProgress(totalChunks int, totalChunkSize int64) {
	operator.totalChunks = totalChunks
	atomic.StoreInt64(&operator.totalChunkSize, totalChunkSize)
	atomic.StoreInt64(&operator.downloadedChunkSize, 0)
	atomic.StoreInt64(&operator.remoteDownloadedChunkSize, 0)
	atomic.StoreInt64(&operator.cachedChunkCount, 0)
	atomic.StoreInt64(&operator.remoteDownloadStartTime, 0)
}

// AddSkippedChunk accounts for a chunk that does not need to be downloaded (e.g., belongs to a
// skipped file or already exists locally in an existing file) so that restore progress percentage
// reflects the true position across the entire restore.
func (operator *ChunkOperator) AddSkippedChunk(chunkSize int64) {
	if chunkSize != 0 {
		atomic.AddInt64(&operator.downloadedChunkSize, chunkSize)
	}
}

func (operator *ChunkOperator) Stop() {
	if atomic.LoadInt64(&operator.numberOfActiveTasks) < 0 {
		return
	}

	for atomic.LoadInt64(&operator.numberOfActiveTasks) > 0 {
		time.Sleep(100 * time.Millisecond)
	}
	for i := 0; i < operator.threads; i++ {
		operator.stopChannel <- false
	}

	// Assign -1 to numberOfActiveTasks so Stop() can be called multiple times
	atomic.AddInt64(&operator.numberOfActiveTasks, int64(-1))
}

func (operator *ChunkOperator) WaitForCompletion() {

	for atomic.LoadInt64(&operator.numberOfActiveTasks) > 0 {
		time.Sleep(100 * time.Millisecond)
	}
}

func (operator *ChunkOperator) AddTask(operation int, chunkID string, chunkHash string, filePath string, chunkIndex int, chunk *Chunk, isMetadata bool, skipCache bool, completionFunc func(*Chunk, int))  {

	task := ChunkTask {
		operation:  operation,
		chunkID:    chunkID,
		chunkHash:  chunkHash,
		chunkIndex: chunkIndex,
		filePath:   filePath,
		chunk:      chunk,
		isMetadata: isMetadata,
		skipCache:  skipCache,
		completionFunc: completionFunc,
	}

	operator.taskQueue <- task
	atomic.AddInt64(&operator.numberOfActiveTasks, int64(1))

	return
}

func (operator *ChunkOperator) Download(chunkHash string, chunkIndex int, isMetadata bool) *Chunk {
	return operator.download(chunkHash, chunkIndex, isMetadata, false)
}

// DownloadFromStorage downloads a chunk from the storage, bypassing the snapshot cache.  This is needed when
// the copy of the chunk in the storage is to be verified.
func (operator *ChunkOperator) DownloadFromStorage(chunkHash string, chunkIndex int, isMetadata bool) *Chunk {
	return operator.download(chunkHash, chunkIndex, isMetadata, true)
}

func (operator *ChunkOperator) download(chunkHash string, chunkIndex int, isMetadata bool, skipCache bool) *Chunk {
	chunkID := operator.config.GetChunkIDFromHash(chunkHash)
	completionChannel := make(chan *Chunk)
	completionFunc := func(chunk *Chunk, chunkIndex int) {
		completionChannel <- chunk
	}
	operator.AddTask(ChunkOperationDownload, chunkID, chunkHash, "", chunkIndex, nil, isMetadata, skipCache, completionFunc)
	return <- completionChannel
}

func (operator *ChunkOperator) DownloadAsync(chunkHash string, chunkIndex int, isMetadata bool, completionFunc func(*Chunk, int)) {
	chunkID := operator.config.GetChunkIDFromHash(chunkHash)
	operator.AddTask(ChunkOperationDownload, chunkID, chunkHash, "", chunkIndex, nil, isMetadata, false, completionFunc)
}

func (operator *ChunkOperator) Upload(chunk *Chunk, chunkIndex int, isMetadata bool) {
	chunkHash := chunk.GetHash()
	chunkID := operator.config.GetChunkIDFromHash(chunkHash)
	operator.AddTask(ChunkOperationUpload, chunkID, chunkHash, "", chunkIndex, chunk, isMetadata, false, nil)
}

// Delete deletes the chunk file at 'filePath'.  If 'filePath' is empty the chunk will be located by its id,
// under the metadata directory if 'isMetadata' is true.
func (operator *ChunkOperator) Delete(chunkID string, filePath string, isMetadata bool) {
	operator.AddTask(ChunkOperationDelete, chunkID, "", filePath, 0, nil, isMetadata, false, nil)
}

// Fossilize turns the chunk file at 'filePath' into a fossil.  If 'filePath' is empty the chunk will be
// located by its id, under the metadata directory if 'isMetadata' is true.
func (operator *ChunkOperator) Fossilize(chunkID string, filePath string, isMetadata bool) {
	operator.AddTask(ChunkOperationFossilize, chunkID, "", filePath, 0, nil, isMetadata, false, nil)
}

// Resurrect turns the fossil at 'filePath' back into a chunk.  The chunk is restored to the directory the
// fossil resides in.
func (operator *ChunkOperator) Resurrect(chunkID string, filePath string) {
	operator.AddTask(ChunkOperationResurrect, chunkID, "", filePath, 0, nil, IsMetadataChunkPath(filePath), false, nil)
}

func (operator *ChunkOperator) Run(threadIndex int, task ChunkTask) {
	defer func() {
		atomic.AddInt64(&operator.numberOfActiveTasks, int64(-1))
	}()

	if task.operation == ChunkOperationDownload {
		operator.DownloadChunk(threadIndex, task)
		return
	} else if task.operation == ChunkOperationUpload {
		operator.UploadChunk(threadIndex, task)
		return
	}

	// task.filePath may be empty.  If so, find the chunk first.
	if task.operation == ChunkOperationDelete || task.operation == ChunkOperationFossilize {
		if task.filePath == "" {
			filePath, exist, _, err := operator.storage.FindChunk(threadIndex, task.chunkID, false, task.isMetadata)
			if err != nil {
				LOG_ERROR("CHUNK_FIND", "Failed to locate the path for the chunk %s: %v", task.chunkID, err)
				return
			} else if !exist {
				if task.operation == ChunkOperationDelete {
					LOG_WARN("CHUNK_FIND", "Chunk %s does not exist in the storage", task.chunkID)
					return
				}

				fossilPath, exist, _, _ := operator.storage.FindChunk(threadIndex, task.chunkID, true, task.isMetadata)
				if exist {
					LOG_WARN("CHUNK_FOSSILIZE", "Chunk %s is already a fossil", task.chunkID)
					operator.collectionLock.Lock()
					operator.fossils = append(operator.fossils, fossilPath)
					operator.collectionLock.Unlock()
				} else {
					LOG_ERROR("CHUNK_FIND", "Chunk %s does not exist in the storage", task.chunkID)
				}
				return
			}
			task.filePath = filePath
		}
	}

	if task.operation == ChunkOperationFind {
		_, exist, _, err := operator.storage.FindChunk(threadIndex, task.chunkID, false, task.isMetadata)
		if err != nil {
			LOG_ERROR("CHUNK_FIND", "Failed to locate the path for the chunk %s: %v", task.chunkID, err)
		} else if !exist {
			LOG_ERROR("CHUNK_FIND", "Chunk %s does not exist in the storage", task.chunkID)
		} else {
			LOG_DEBUG("CHUNK_FIND", "Chunk %s exists in the storage", task.chunkID)
		}
	} else if task.operation == ChunkOperationDelete {
		err := operator.storage.DeleteFile(threadIndex, task.filePath)
		if err != nil {
			LOG_WARN("CHUNK_DELETE", "Failed to remove the file %s: %v", task.filePath, err)
		} else {
			if task.chunkID != "" {
				LOG_INFO("CHUNK_DELETE", "The chunk %s has been permanently removed", task.chunkID)
			} else {
				LOG_INFO("CHUNK_DELETE", "Deleted file %s from the storage", task.filePath)
			}
		}
	} else if task.operation == ChunkOperationFossilize {

		fossilPath := task.filePath + ".fsl"

		err := operator.storage.MoveFile(threadIndex, task.filePath, fossilPath)
		if err != nil {
			if _, exist, _, _ := operator.storage.FindChunk(threadIndex, task.chunkID, true, task.isMetadata); exist {
				err := operator.storage.DeleteFile(threadIndex, task.filePath)
				if err == nil {
					LOG_TRACE("CHUNK_DELETE", "Deleted chunk file %s as the fossil already exists", task.chunkID)
				}
				operator.collectionLock.Lock()
				operator.fossils = append(operator.fossils, fossilPath)
				operator.collectionLock.Unlock()
			} else {
				LOG_ERROR("CHUNK_DELETE", "Failed to fossilize the chunk %s: %v", task.chunkID, err)
			}
		} else {
			LOG_TRACE("CHUNK_FOSSILIZE", "The chunk %s has been marked as a fossil", task.chunkID)
			operator.collectionLock.Lock()
			operator.fossils = append(operator.fossils, fossilPath)
			operator.collectionLock.Unlock()
		}
	} else if task.operation == ChunkOperationResurrect {
		chunkPath, exist, _, err := operator.storage.FindChunk(threadIndex, task.chunkID, false, task.isMetadata)
		if err != nil {
			LOG_ERROR("CHUNK_FIND", "Failed to locate the path for the chunk %s: %v", task.chunkID, err)
		}

		if exist {
			operator.storage.DeleteFile(threadIndex, task.filePath)
			LOG_INFO("FOSSIL_RESURRECT", "The chunk %s already exists", task.chunkID)
		} else {
			err := operator.storage.MoveFile(threadIndex, task.filePath, chunkPath)
			if err != nil {
				LOG_ERROR("FOSSIL_RESURRECT", "Failed to resurrect the chunk %s from the fossil %s: %v",
					task.chunkID, task.filePath, err)
			} else {
				LOG_INFO("FOSSIL_RESURRECT", "The chunk %s has been resurrected", task.filePath)
			}
		}
	}
}

// Download downloads a chunk from the storage.
func (operator *ChunkOperator) DownloadChunk(threadIndex int, task ChunkTask) {

	cachedPath := ""
	chunk := operator.config.GetChunk()
	chunk.isMetadata = task.isMetadata
	chunkID := task.chunkID

	defer func() {
		if chunk != nil {
			operator.config.PutChunk(chunk)
		}
	} ()

	// The local cache is always used for metadata chunks.  For data chunks it is only used when
	// explicitly enabled, because it stores the chunks unencrypted on the local disk.
	if (task.isMetadata || operator.cacheDataChunks) && operator.snapshotCache != nil && !task.skipCache {

		var exist bool
		var err error

		// Reset the chunk with a hasher -- we're reading from the cache where chunk are not encrypted or compressed
		chunk.Reset(true)

		cachedPath, exist, _, err = operator.snapshotCache.FindChunk(threadIndex, chunkID, false, task.isMetadata)
		if err != nil {
			LOG_WARN("DOWNLOAD_CACHE", "Failed to find the cache path for the chunk %s: %v", chunkID, err)
		} else if exist {
			err = operator.snapshotCache.DownloadFile(0, cachedPath, chunk)
			if err != nil {
				LOG_WARN("DOWNLOAD_CACHE", "Failed to load the chunk %s from the local cache: %v", chunkID, err)
			} else {
				actualChunkID := chunk.GetID()
				if actualChunkID != chunkID {
					LOG_WARN("DOWNLOAD_CACHE_CORRUPTED",
						"The chunk %s loaded from the local cache has a hash id of %s", chunkID, actualChunkID)
					// Remove the corrupted cache file so that it won't be loaded again
					operator.snapshotCache.DeleteFile(threadIndex, cachedPath)
				} else {
					LOG_DEBUG("CHUNK_CACHE", "Chunk %s has been loaded from the local cache", chunkID)

					// A data chunk loaded from the local cache counts as completed so that the
					// progress percentage keeps advancing and can reach 100% (metadata chunks are
					// excluded from totalChunkSize during restore).
					if !task.isMetadata {
						atomic.AddInt64(&operator.cachedChunkCount, 1)
						completedChunkSize := atomic.AddInt64(&operator.downloadedChunkSize, int64(chunk.GetLength()))
						totalChunkSize := atomic.LoadInt64(&operator.totalChunkSize)
						if (operator.showStatistics || IsTracing()) && totalChunkSize > 0 {
							if completedChunkSize > totalChunkSize {
								completedChunkSize = totalChunkSize
							}
							percentage := float32(completedChunkSize * 1000 / totalChunkSize)
							if operator.totalChunks > 0 {
								LOG_INFO("DOWNLOAD_PROGRESS", "Cached chunk %d/%d size %d, %.1f%%",
									task.chunkIndex+1, operator.totalChunks, chunk.GetLength(), percentage/10)
							} else {
								LOG_INFO("DOWNLOAD_PROGRESS", "Cached chunk %d size %d, %.1f%%",
									task.chunkIndex+1, chunk.GetLength(), percentage/10)
							}
						}
					}

					task.completionFunc(chunk, task.chunkIndex)
					chunk = nil
					return
				}
			}
		}
	}

	// Reset the chunk without a hasher -- the downloaded content will be encrypted and/or compressed and the hasher
	// will be set up before the encryption
	chunk.Reset(false)
	chunk.isMetadata = task.isMetadata

	// If failures are allowed, complete the task properly
	completeFailedChunk := func() {

		atomic.AddInt64(&operator.NumberOfFailedChunks, 1)
		if operator.allowFailures {
			chunk.isBroken = true
			task.completionFunc(chunk, task.chunkIndex)
		}
	}

	chunkPath := ""
	fossilPath := ""
	filePath := ""

	const MaxDownloadAttempts = 3
	const MaxStalledResumes = 3
	const MaxTotalResumes = 16
	if !task.isMetadata {
		atomic.CompareAndSwapInt64(&operator.remoteDownloadStartTime, 0, time.Now().Unix())
	}
	for downloadAttempt := 0; ; downloadAttempt++ {

		exist := false
		var err error

		// Find the chunk by ID first.
		chunkPath, exist, _, err = operator.storage.FindChunk(threadIndex, chunkID, false, task.isMetadata)
		if err != nil {
			if IsTransientNetworkError(err) && downloadAttempt < MaxDownloadAttempts {
				LOG_WARN("DOWNLOAD_RETRY", "Failed to find the chunk %s: %v; retrying", chunkID, err)
				time.Sleep(time.Duration(downloadAttempt+1) * downloadRetryDelay)
				continue
			}
			completeFailedChunk()
			LOG_WERROR(operator.allowFailures, "DOWNLOAD_CHUNK", "Failed to find the chunk %s: %v", chunkID, err)
			return
		}

		if exist {
			filePath = chunkPath
		} else {
			// No chunk is found.  Have to find it in the fossil pool again.
			fossilPath, exist, _, err = operator.storage.FindChunk(threadIndex, chunkID, true, task.isMetadata)
			if err != nil {
				if IsTransientNetworkError(err) && downloadAttempt < MaxDownloadAttempts {
					LOG_WARN("DOWNLOAD_RETRY", "Failed to find the fossil of the chunk %s: %v; retrying", chunkID, err)
					time.Sleep(time.Duration(downloadAttempt+1) * downloadRetryDelay)
					continue
				}
				completeFailedChunk()
				LOG_WERROR(operator.allowFailures, "DOWNLOAD_CHUNK", "Failed to find the chunk %s: %v", chunkID, err)
				return
			}

			if !exist {

				retry := false

				// Retry for Hubic or WebDAV as it may return 404 even when the chunk exists
				if _, ok := operator.storage.(*HubicStorage); ok {
					retry = true
				}

				if _, ok := operator.storage.(*WebDAVStorage); ok {
					retry = true
				}

				if retry && downloadAttempt < MaxDownloadAttempts {
					LOG_WARN("DOWNLOAD_RETRY", "Failed to find the chunk %s; retrying", chunkID)
					continue
				}

				// A chunk is not found.  This is a serious error and hopefully it will never happen.
				completeFailedChunk()
				if err != nil {
					LOG_WERROR(operator.allowFailures, "DOWNLOAD_CHUNK", "Chunk %s can't be found: %v", chunkID, err)
				} else {
					LOG_WERROR(operator.allowFailures, "DOWNLOAD_CHUNK", "Chunk %s can't be found", chunkID)
				}
				return
			}

			filePath = fossilPath
			LOG_WARN("DOWNLOAD_FOSSIL", "Chunk %s is a fossil", chunkID)
		}

		err = operator.storage.DownloadFile(threadIndex, filePath, chunk)
		if err != nil {
			// Some bytes may have been received before the failure.  If the storage supports ranged
			// downloads and the error is transient, keep resuming from where it left off.
			resumed := false
			rangeUnsupported := false
			if chunk.GetLength() > 0 && (IsTransientNetworkError(err) || err == errWebDAVMaximumBackoff) {
				if resumableStorage, ok := operator.storage.(RangeDownloadStorage); ok {
					stalledResumes := 0
					for totalResumes := 0; totalResumes < MaxTotalResumes && stalledResumes < MaxStalledResumes; totalResumes++ {
						offset := int64(chunk.GetLength())
						LOG_WARN("DOWNLOAD_RESUME", "Failed to download the chunk %s: %v; resuming from byte %d",
							chunkID, err, offset)
						time.Sleep(time.Duration(stalledResumes+1) * downloadRetryDelay)

						err = resumableStorage.DownloadFileRange(threadIndex, filePath, offset, chunk)
						if err == nil {
							resumed = true
							break
						}
						if errors.Is(err, ErrRangeNotSatisfiable) {
							// All bytes may already have been received before a trailing read error;
							// let decryption and hash verification decide if the chunk is complete.
							resumed = true
							err = nil
							break
						}
						if errors.Is(err, ErrRangeNotSupported) {
							LOG_WARN("DOWNLOAD_RESUME", "Storage does not support range downloads for chunk %s; falling back to full retry", chunkID)
							rangeUnsupported = true
							break
						}
						if !IsTransientNetworkError(err) && err != errWebDAVMaximumBackoff {
							break
						}
						if int64(chunk.GetLength()) > offset {
							// Forward progress was made; reset consecutive stalled count
							stalledResumes = 0
						} else {
							stalledResumes++
						}
					}
				}
			}

			if !resumed {
				_, isHubic := operator.storage.(*HubicStorage)
				// Retry on transient network errors, unsupported range fallback, or if it is a
				// Hubic/WebDAV backend as its requests may fail even when the chunk exists
				if (rangeUnsupported || IsTransientNetworkError(err) || isHubic || err == errWebDAVMaximumBackoff) && downloadAttempt < MaxDownloadAttempts {
					LOG_WARN("DOWNLOAD_RETRY", "Failed to download the chunk %s: %v; retrying", chunkID, err)
					chunk.Reset(false)
					chunk.isMetadata = task.isMetadata
					time.Sleep(time.Duration(downloadAttempt+1) * downloadRetryDelay)
					continue
				} else {
					completeFailedChunk()
					LOG_WERROR(operator.allowFailures, "DOWNLOAD_CHUNK", "Failed to download the chunk %s: %v", chunkID, err)
					return
				}
			}
		}

		rewriteNeeded := false
		err, rewriteNeeded = chunk.Decrypt(operator.config.ChunkKey, task.chunkHash)
		if err != nil {
			if downloadAttempt < MaxDownloadAttempts {
				LOG_WARN("DOWNLOAD_RETRY", "Failed to decrypt the chunk %s: %v; retrying", chunkID, err)
				chunk.Reset(false)
				chunk.isMetadata = task.isMetadata
				time.Sleep(time.Duration(downloadAttempt+1) * downloadRetryDelay)
				continue
			} else {
				completeFailedChunk()
				LOG_WERROR(operator.allowFailures, "DOWNLOAD_DECRYPT", "Failed to decrypt the chunk %s: %v", chunkID, err)
				return
			}
		}

		actualChunkID := chunk.GetID()
		if actualChunkID != chunkID {
			if downloadAttempt < MaxDownloadAttempts {
				LOG_WARN("DOWNLOAD_RETRY", "The chunk %s has a hash id of %s; retrying", chunkID, actualChunkID)
				chunk.Reset(false)
				chunk.isMetadata = task.isMetadata
				time.Sleep(time.Duration(downloadAttempt+1) * downloadRetryDelay)
				continue
			} else {
				completeFailedChunk()
				LOG_WERROR(operator.allowFailures, "DOWNLOAD_CORRUPTED", "The chunk %s has a hash id of %s", chunkID, actualChunkID)
				return
			}
		}

		if rewriteNeeded && operator.rewriteChunks {

			if filePath != fossilPath {
				fossilPath = filePath + ".fsl"
				err := operator.storage.MoveFile(threadIndex, chunkPath, fossilPath)
				if err != nil {
					LOG_WARN("CHUNK_REWRITE", "Failed to fossilize the chunk %s: %v", task.chunkID, err)
				} else {
					LOG_TRACE("CHUNK_REWRITE", "The existing chunk %s has been marked as a fossil for rewrite", task.chunkID)
					operator.collectionLock.Lock()
					operator.fossils = append(operator.fossils, fossilPath)
					operator.collectionLock.Unlock()
				}
			}

			newChunk := operator.config.GetChunk()
			newChunk.Reset(true)
			newChunk.Write(chunk.GetBytes())
			// Encrypt the chunk only after we know that it must be uploaded.
			err = newChunk.Encrypt(operator.config.ChunkKey, chunk.GetHash(), task.isMetadata)
			if err == nil {
				// Re-upload the chunk
				err = operator.storage.UploadFile(threadIndex, chunkPath, newChunk.GetBytes())
				if err != nil {
					LOG_WARN("CHUNK_REWRITE", "Failed to re-upload the chunk %s: %v", chunkID, err)
				} else {
					LOG_INFO("CHUNK_REWRITE", "The chunk %s has been re-uploaded", chunkID)
				}
			}
			operator.config.PutChunk(newChunk)
		}

		break
	}

	if (chunk.isMetadata || operator.cacheDataChunks) && len(cachedPath) > 0 {
		// Save a copy to the local cache
		err := operator.snapshotCache.UploadFile(threadIndex, cachedPath, chunk.GetBytes())
		if err != nil {
			LOG_WARN("DOWNLOAD_CACHE", "Failed to add the chunk %s to the local cache: %v", chunkID, err)
		}
	}

	if !task.isMetadata {
		completedChunkSize := atomic.AddInt64(&operator.downloadedChunkSize, int64(chunk.GetLength()))
		remoteDownloadedSize := atomic.AddInt64(&operator.remoteDownloadedChunkSize, int64(chunk.GetLength()))
		totalChunkSize := atomic.LoadInt64(&operator.totalChunkSize)

		if (operator.showStatistics || IsTracing()) && totalChunkSize > 0 {
			startTime := atomic.LoadInt64(&operator.remoteDownloadStartTime)
			if startTime == 0 {
				startTime = operator.startTime
			}
			now := time.Now().Unix()
			if now <= startTime {
				now = startTime + 1
			}
			speed := remoteDownloadedSize / (now - startTime)
			if completedChunkSize > totalChunkSize {
				completedChunkSize = totalChunkSize
			}
			remainingSize := totalChunkSize - completedChunkSize
			remainingTime := int64(0)
			if speed > 0 && remainingSize > 0 {
				remainingTime = remainingSize/speed + 1
			}
			percentage := float32(completedChunkSize * 1000 / totalChunkSize)
			if operator.totalChunks > 0 {
				LOG_INFO("DOWNLOAD_PROGRESS", "Downloaded chunk %d/%d size %d, %sB/s %s %.1f%%",
					task.chunkIndex+1, operator.totalChunks, chunk.GetLength(),
					PrettySize(speed), PrettyTime(remainingTime), percentage/10)
			} else {
				LOG_INFO("DOWNLOAD_PROGRESS", "Downloaded chunk %d size %d, %sB/s %s %.1f%%",
					task.chunkIndex+1, chunk.GetLength(),
					PrettySize(speed), PrettyTime(remainingTime), percentage/10)
			}
		} else {
			LOG_DEBUG("CHUNK_DOWNLOAD", "Chunk %s has been downloaded", chunkID)
		}
	} else {
		LOG_DEBUG("CHUNK_DOWNLOAD", "Chunk %s has been downloaded", chunkID)
	}

	task.completionFunc(chunk, task.chunkIndex)
	chunk = nil
	return
}

// UploadChunk is called by the task goroutines to perform the actual uploading
func (operator *ChunkOperator) UploadChunk(threadIndex int, task ChunkTask) bool {

	chunk := task.chunk
	chunkID := task.chunkID
	chunkSize := chunk.GetLength()

	// The data in the buffer is checked against the chunk id by Encrypt() below

	if task.isMetadata && operator.snapshotCache != nil && operator.storage.IsCacheNeeded() {
		// Save a copy to the local snapshot.
		chunkPath, exist, _, err := operator.snapshotCache.FindChunk(threadIndex, chunkID, false, task.isMetadata)
		if err != nil {
			LOG_WARN("UPLOAD_CACHE", "Failed to find the cache path for the chunk %s: %v", chunkID, err)
		} else if exist {
			LOG_DEBUG("CHUNK_CACHE", "Chunk %s already exists in the snapshot cache", chunkID)
		} else if err = operator.snapshotCache.UploadFile(threadIndex, chunkPath, chunk.GetBytes()); err != nil {
			LOG_WARN("UPLOAD_CACHE", "Failed to save the chunk %s to the snapshot cache: %v", chunkID, err)
		} else {
			LOG_DEBUG("CHUNK_CACHE", "Chunk %s has been saved to the snapshot cache", chunkID)
		}
	}

	// This returns the path the chunk file should be at.
	chunkPath, exist, _, err := operator.storage.FindChunk(threadIndex, chunkID, false, task.isMetadata)
	if err != nil {
		LOG_ERROR("UPLOAD_CHUNK", "Failed to find the path for the chunk %s: %v", chunkID, err)
		return false
	}

	if exist {
		// Chunk deduplication by name in effect here.
		LOG_DEBUG("CHUNK_DUPLICATE", "Chunk %s already exists", chunkID)

		operator.UploadCompletionFunc(chunk, task.chunkIndex, false, chunkSize, 0)
		return false
	}

	// Encrypt the chunk only after we know that it must be uploaded.
	err = chunk.Encrypt(operator.config.ChunkKey, chunk.GetHash(), task.isMetadata)
	if err != nil {
		LOG_ERROR("UPLOAD_CHUNK", "Failed to encrypt the chunk %s: %v", chunkID, err)
		return false
	}

	if !operator.config.dryRun {
		err = operator.storage.UploadFile(threadIndex, chunkPath, chunk.GetBytes())
		if err != nil {
			LOG_ERROR("UPLOAD_CHUNK", "Failed to upload the chunk %s: %v", chunkID, err)
			return false
		}
		LOG_DEBUG("CHUNK_UPLOAD", "Chunk %s has been uploaded", chunkID)
	} else {
		LOG_DEBUG("CHUNK_UPLOAD", "Uploading was skipped for chunk %s", chunkID)
	}

	operator.UploadCompletionFunc(chunk, task.chunkIndex, false, chunkSize, chunk.GetLength())
	return true
}