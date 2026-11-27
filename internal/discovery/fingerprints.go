package discovery

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"os"
	"time"
)

const (
	fingerprintWindowBytes = 4096
	sqliteHeaderMagic      = "SQLite format 3\x00"
)

// FileFingerprint captures size, mtime, inode/device, and 4 KiB head/tail hashes for JSONL files.
type FileFingerprint struct {
	Size       int64  `json:"size"`
	MtimeNs    int64  `json:"mtime_ns"`
	Path       string `json:"path"`
	Device     uint64 `json:"device,omitempty"`
	Inode      uint64 `json:"inode,omitempty"`
	HasDevIno  bool   `json:"has_dev_ino,omitempty"`
	HeadLength int    `json:"head_length"`
	HeadHash   string `json:"head_hash"`
	TailOffset int64  `json:"tail_offset"`
	TailLength int    `json:"tail_length"`
	TailHash   string `json:"tail_hash"`
}

// ComputeFileFingerprint reads bounded head and tail slices to detect truncation or same-size rewrites.
func ComputeFileFingerprint(path string) (FileFingerprint, error) {
	info, err := os.Stat(path)
	if err != nil {
		return FileFingerprint{}, err
	}
	size := info.Size()
	window := size
	if window > fingerprintWindowBytes {
		window = fingerprintWindowBytes
	}
	tailOffset := size - window

	f, err := os.Open(path)
	if err != nil {
		return FileFingerprint{}, err
	}
	defer func() { _ = f.Close() }()

	head := make([]byte, window)
	nHead, err := io.ReadFull(f, head)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return FileFingerprint{}, err
	}
	head = head[:nHead]

	if _, err := f.Seek(tailOffset, io.SeekStart); err != nil {
		return FileFingerprint{}, err
	}
	tail := make([]byte, window)
	nTail, err := io.ReadFull(f, tail)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return FileFingerprint{}, err
	}
	tail = tail[:nTail]

	dev, ino, hasDevIno := statDevIno(info)
	headSum := sha256.Sum256(head)
	tailSum := sha256.Sum256(tail)

	return FileFingerprint{
		Size:       size,
		MtimeNs:    info.ModTime().UnixNano(),
		Path:       CachedResolve(path),
		Device:     dev,
		Inode:      ino,
		HasDevIno:  hasDevIno,
		HeadLength: len(head),
		HeadHash:   hex.EncodeToString(headSum[:]),
		TailOffset: tailOffset,
		TailLength: len(tail),
		TailHash:   hex.EncodeToString(tailSum[:]),
	}, nil
}

// IndexedContentMatches verifies inode/device and previously hashed head/tail windows still match.
func IndexedContentMatches(path string, prev FileFingerprint) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	if prev.HasDevIno {
		dev, ino, ok := statDevIno(info)
		if ok && (dev != prev.Device || ino != prev.Inode) {
			return false
		}
	}
	if prev.HeadHash == "" {
		return true
	}
	if info.Size() < int64(prev.HeadLength) || info.Size() < prev.TailOffset+int64(prev.TailLength) {
		return false
	}

	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()

	head := make([]byte, prev.HeadLength)
	if _, err := io.ReadFull(f, head); err != nil {
		return false
	}
	if _, err := f.Seek(prev.TailOffset, io.SeekStart); err != nil {
		return false
	}
	tail := make([]byte, prev.TailLength)
	if _, err := io.ReadFull(f, tail); err != nil {
		return false
	}

	headSum := sha256.Sum256(head)
	tailSum := sha256.Sum256(tail)
	return hex.EncodeToString(headSum[:]) == prev.HeadHash && hex.EncodeToString(tailSum[:]) == prev.TailHash
}

// SQLiteSignature captures main-file, WAL, and header change counter without opening SQLite or -shm.
type SQLiteSignature struct {
	Size          int64
	MtimeNs       int64
	Inode         uint64
	Device        uint64
	HasWAL        bool
	WALSize       int64
	WALMtimeNs    int64
	WALInode      uint64
	ChangeCounter *uint32
}

// Equal reports whether two SQLite signatures match.
func (s SQLiteSignature) Equal(other SQLiteSignature) bool {
	if s.Size != other.Size ||
		s.MtimeNs != other.MtimeNs ||
		s.Inode != other.Inode ||
		s.Device != other.Device ||
		s.HasWAL != other.HasWAL ||
		s.WALSize != other.WALSize ||
		s.WALMtimeNs != other.WALMtimeNs ||
		s.WALInode != other.WALInode {
		return false
	}
	if (s.ChangeCounter == nil) != (other.ChangeCounter == nil) {
		return false
	}
	if s.ChangeCounter != nil && *s.ChangeCounter != *other.ChangeCounter {
		return false
	}
	return true
}

// IsRacy reports whether the main database or WAL mtime falls within window of now.
func (s SQLiteSignature) IsRacy(now time.Time, window time.Duration) bool {
	absSub := func(ns int64) time.Duration {
		d := now.Sub(time.Unix(0, ns))
		if d < 0 {
			return -d
		}
		return d
	}
	if absSub(s.MtimeNs) < window {
		return true
	}
	return s.HasWAL && absSub(s.WALMtimeNs) < window
}

// SQLiteChangeCounter reads the 4-byte change counter at offset 24 without triggering -shm creation.
func SQLiteChangeCounter(path string) *uint32 {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer func() { _ = f.Close() }()

	var hdr [28]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return nil
	}
	if string(hdr[:16]) != sqliteHeaderMagic {
		return nil
	}
	val := binary.BigEndian.Uint32(hdr[24:28])
	return &val
}

// ReadSQLiteSignature computes a change signature for a read-only SQLite database without reading -shm.
func ReadSQLiteSignature(path string) (SQLiteSignature, error) {
	info, err := os.Stat(path)
	if err != nil {
		return SQLiteSignature{}, err
	}
	dev, ino, _ := statDevIno(info)
	sig := SQLiteSignature{
		Size:          info.Size(),
		MtimeNs:       info.ModTime().UnixNano(),
		Inode:         ino,
		Device:        dev,
		ChangeCounter: SQLiteChangeCounter(path),
	}
	if walInfo, walErr := os.Stat(path + "-wal"); walErr == nil {
		_, walIno, _ := statDevIno(walInfo)
		sig.HasWAL = true
		sig.WALSize = walInfo.Size()
		sig.WALMtimeNs = walInfo.ModTime().UnixNano()
		sig.WALInode = walIno
	}
	return sig, nil
}
