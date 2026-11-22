// Package chunker implements 512 KiB chunking, bitfield bookkeeping,
// SHA-256 integrity checks and multi-source swarm downloads.
//
// The swarm scheduler and integrity pipeline are owned by the Chunking module
// owner (Member 4). The fetcher is injected by the transport/room layer so this
// package stays free of networking concerns.
package chunker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/huykhanh2141/croc-p2p/pkg/protocol"
)

// FetchFunc fetches one chunk from a source peer. It is implemented by the
// transport/room integration and injected via SetFetcher.
type FetchFunc func(ctx context.Context, source protocol.PeerInfo, req protocol.ChunkRequest) (protocol.ChunkData, error)

// Manager is the default protocol.ChunkManager implementation.
var _ protocol.ChunkManager = (*Manager)(nil)

// Manager keeps per-file state, bitfields and the injected chunk fetcher.
type Manager struct {
	mu      sync.RWMutex
	files   map[string]*fileState
	fetch   FetchFunc
	workers int
}

type fileState struct {
	meta       protocol.FileMeta
	sourcePath string

	mu     sync.Mutex
	bits   *bitset.BitSet
	chunks map[uint32][]byte
}

// New returns a chunk manager with a modest download worker pool.
func New() *Manager {
	return &Manager{
		files:   make(map[string]*fileState),
		workers: 8,
	}
}

// SetFetcher injects the transport-backed chunk fetcher.
func (m *Manager) SetFetcher(fetch FetchFunc) {
	m.mu.Lock()
	m.fetch = fetch
	m.mu.Unlock()
}

// SetWorkers changes the download concurrency (values < 1 are clamped to 1).
func (m *Manager) SetWorkers(n int) {
	if n < 1 {
		n = 1
	}
	m.mu.Lock()
	m.workers = n
	m.mu.Unlock()
}

// PrepareFile hashes a local file and registers its chunk layout.
func (m *Manager) PrepareFile(path string) (protocol.FileMeta, error) {
	file, err := os.Open(path)
	if err != nil {
		return protocol.FileMeta{}, fmt.Errorf("chunker: open %s: %w", path, err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return protocol.FileMeta{}, fmt.Errorf("chunker: stat %s: %w", path, err)
	}
	if info.IsDir() {
		return protocol.FileMeta{}, fmt.Errorf("chunker: %s is a directory: %w", path, protocol.ErrInvalidMessage)
	}

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return protocol.FileMeta{}, fmt.Errorf("chunker: hash %s: %w", path, err)
	}
	digest := hex.EncodeToString(hasher.Sum(nil))

	chunkSize := protocol.ChunkSize
	numChunks := uint32(0)
	if info.Size() > 0 {
		numChunks = uint32((info.Size() + int64(chunkSize) - 1) / int64(chunkSize))
	}

	meta := protocol.FileMeta{
		ID:        digest,
		Name:      filepath.Base(path),
		Size:      info.Size(),
		ChunkSize: chunkSize,
		NumChunks: numChunks,
		SHA256:    digest,
		CreatedAt: time.Now().UTC(),
	}
	state := &fileState{
		meta:       meta,
		sourcePath: path,
		bits:       bitset.New(uint(numChunks)),
		chunks:     make(map[uint32][]byte),
	}

	m.mu.Lock()
	m.files[meta.ID] = state
	m.mu.Unlock()

	return meta, nil
}

// ReadChunk returns a chunk from the downloaded cache or the source file.
func (m *Manager) ReadChunk(fileID string, index uint32) ([]byte, error) {
	state, err := m.state(fileID)
	if err != nil {
		return nil, err
	}
	if index >= state.meta.NumChunks {
		return nil, fmt.Errorf("chunker: chunk %d out of range (0..%d): %w", index, state.meta.NumChunks-1, protocol.ErrInvalidMessage)
	}

	state.mu.Lock()
	if data, ok := state.chunks[index]; ok {
		copied := append([]byte(nil), data...)
		state.mu.Unlock()
		return copied, nil
	}
	state.mu.Unlock()

	if state.sourcePath == "" {
		return nil, fmt.Errorf("chunker: chunk %d of %s not downloaded: %w", index, fileID, protocol.ErrChunkUnavailable)
	}

	file, err := os.Open(state.sourcePath)
	if err != nil {
		return nil, fmt.Errorf("chunker: open %s: %w", state.sourcePath, err)
	}
	defer file.Close()

	offset := int64(index) * int64(state.meta.ChunkSize)
	length := int64(state.meta.ChunkSize)
	if remaining := state.meta.Size - offset; remaining < length {
		length = remaining
	}
	if length <= 0 {
		return []byte{}, nil
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("chunker: seek chunk %d: %w", index, err)
	}

	buf := make([]byte, length)
	n, err := io.ReadFull(file, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("chunker: read chunk %d: %w", index, err)
	}
	return buf[:n], nil
}

// VerifyChunk compares data against a hex-encoded SHA-256 digest.
func (m *Manager) VerifyChunk(data []byte, expectedSHA256 string) bool {
	if expectedSHA256 == "" {
		return false
	}
	sum := sha256.Sum256(data)
	return strings.EqualFold(hex.EncodeToString(sum[:]), expectedSHA256)
}

// SetBit marks a chunk as owned.
func (m *Manager) SetBit(fileID string, index uint32) error {
	state, err := m.state(fileID)
	if err != nil {
		return err
	}
	if index >= state.meta.NumChunks {
		return fmt.Errorf("chunker: chunk %d out of range: %w", index, protocol.ErrInvalidMessage)
	}
	state.mu.Lock()
	state.bits.Set(uint(index))
	state.mu.Unlock()
	return nil
}

// ClearBit marks a chunk as missing.
func (m *Manager) ClearBit(fileID string, index uint32) error {
	state, err := m.state(fileID)
	if err != nil {
		return err
	}
	if index >= state.meta.NumChunks {
		return fmt.Errorf("chunker: chunk %d out of range: %w", index, protocol.ErrInvalidMessage)
	}
	state.mu.Lock()
	state.bits.Clear(uint(index))
	state.mu.Unlock()
	return nil
}

// Bitfield returns the binary-marshalled bitset.
func (m *Manager) Bitfield(fileID string) ([]byte, error) {
	state, err := m.state(fileID)
	if err != nil {
		return nil, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()

	data, err := state.bits.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("chunker: marshal bitfield: %w", err)
	}
	return data, nil
}

// IsComplete reports whether every chunk is owned.
func (m *Manager) IsComplete(fileID string) bool {
	state, err := m.state(fileID)
	if err != nil {
		return false
	}
	if state.meta.NumChunks == 0 {
		return true
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.bits.Count() >= uint(state.meta.NumChunks)
}

// AssembleFile writes all chunks to outputPath and verifies the whole-file
// SHA-256 before returning success.
func (m *Manager) AssembleFile(fileID, outputPath string) error {
	state, err := m.state(fileID)
	if err != nil {
		return err
	}
	if !m.IsComplete(fileID) {
		return fmt.Errorf("chunker: %s is missing chunks: %w", fileID, protocol.ErrFileIncomplete)
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("chunker: create %s: %w", outputPath, err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()

	hasher := sha256.New()
	var written int64
	for index := uint32(0); index < state.meta.NumChunks; index++ {
		data, err := m.ReadChunk(fileID, index)
		if err != nil {
			return err
		}
		n, err := file.Write(data)
		if err != nil {
			return fmt.Errorf("chunker: write chunk %d: %w", index, err)
		}
		written += int64(n)
		_, _ = hasher.Write(data)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("chunker: sync %s: %w", outputPath, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("chunker: close %s: %w", outputPath, err)
	}
	closed = true

	if state.meta.Size > 0 && written != state.meta.Size {
		return fmt.Errorf("chunker: wrote %d bytes, expected %d: %w", written, state.meta.Size, protocol.ErrFileIncomplete)
	}
	actual := hex.EncodeToString(hasher.Sum(nil))
	if state.meta.SHA256 != "" && !strings.EqualFold(actual, state.meta.SHA256) {
		return fmt.Errorf("chunker: assembled sha256 %s != %s: %w", actual, state.meta.SHA256, protocol.ErrChunkHashMismatch)
	}
	return nil
}

// Download fetches missing chunks from multiple sources in parallel, verifies
// per-chunk SHA-256 from ChunkData and updates the bitfield as chunks land.
//
// TODO(member-4): add rarest-first scheduling, source scoring and retry/backoff
// when a peer fails mid-transfer.
func (m *Manager) Download(ctx context.Context, meta protocol.FileMeta, sources []protocol.PeerInfo) error {
	if len(sources) == 0 {
		return fmt.Errorf("chunker: no sources for %s: %w", meta.ID, protocol.ErrP2PUnavailable)
	}

	m.mu.RLock()
	fetch := m.fetch
	workers := m.workers
	m.mu.RUnlock()
	if fetch == nil {
		return fmt.Errorf("chunker: chunk fetcher not configured: %w", protocol.ErrNotImplemented)
	}
	if workers < 1 {
		workers = 1
	}

	state, err := m.ensureState(meta)
	if err != nil {
		return err
	}

	var missing []uint32
	for index := uint32(0); index < state.meta.NumChunks; index++ {
		state.mu.Lock()
		owned := state.bits.Test(uint(index))
		state.mu.Unlock()
		if !owned {
			missing = append(missing, index)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	if workers > len(missing) {
		workers = len(missing)
	}

	jobs := make(chan uint32)
	errCh := make(chan error, len(missing))

	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				select {
				case <-ctx.Done():
					errCh <- ctx.Err()
					return
				default:
				}

				source := sources[int(index)%len(sources)]
				req := protocol.ChunkRequest{
					FileID: meta.ID,
					Index:  index,
					// TODO(member-4): populate RoomID and RequesterID from the
					// room manager context.
				}
				chunk, err := fetch(ctx, source, req)
				if err != nil {
					errCh <- fmt.Errorf("chunker: fetch chunk %d from %s: %w", index, source.ID, err)
					continue
				}
				if chunk.FileID != "" && chunk.FileID != meta.ID {
					errCh <- fmt.Errorf("chunker: chunk %d file id %q mismatch: %w", index, chunk.FileID, protocol.ErrInvalidMessage)
					continue
				}
				if chunk.SHA256 != "" && !m.VerifyChunk(chunk.Data, chunk.SHA256) {
					errCh <- fmt.Errorf("chunker: chunk %d from %s: %w", index, source.ID, protocol.ErrChunkHashMismatch)
					continue
				}

				state.mu.Lock()
				state.chunks[index] = append([]byte(nil), chunk.Data...)
				state.bits.Set(uint(index))
				state.mu.Unlock()
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, index := range missing {
			select {
			case <-ctx.Done():
				return
			case jobs <- index:
			}
		}
	}()

	wg.Wait()
	close(errCh)

	var firstErr error
	for err := range errCh {
		if firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return firstErr
	}
	if !m.IsComplete(meta.ID) {
		return fmt.Errorf("chunker: download of %s incomplete: %w", meta.ID, protocol.ErrFileIncomplete)
	}
	return nil
}

func (m *Manager) state(fileID string) (*fileState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	state, ok := m.files[fileID]
	if !ok {
		return nil, fmt.Errorf("chunker: unknown file %q: %w", fileID, protocol.ErrFileNotFound)
	}
	return state, nil
}

func (m *Manager) ensureState(meta protocol.FileMeta) (*fileState, error) {
	if meta.ID == "" {
		return nil, fmt.Errorf("chunker: empty file id: %w", protocol.ErrInvalidMessage)
	}
	if meta.ChunkSize <= 0 {
		meta.ChunkSize = protocol.ChunkSize
	}
	if meta.NumChunks == 0 && meta.Size > 0 {
		meta.NumChunks = uint32((meta.Size + int64(meta.ChunkSize) - 1) / int64(meta.ChunkSize))
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if state, ok := m.files[meta.ID]; ok {
		return state, nil
	}
	state := &fileState{
		meta:   meta,
		bits:   bitset.New(uint(meta.NumChunks)),
		chunks: make(map[uint32][]byte),
	}
	m.files[meta.ID] = state
	return state, nil
}
