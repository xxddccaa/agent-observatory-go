package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

const maxDecompressedJSONBytes = 64 << 20

var (
	zstdOnce sync.Once
	zstdEnc  *zstd.Encoder
	zstdDec  *zstd.Decoder
	zstdErr  error
)

func initZstd() error {
	zstdOnce.Do(func() {
		zstdEnc, zstdErr = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if zstdErr != nil {
			return
		}
		zstdDec, zstdErr = zstd.NewReader(nil, zstd.WithDecoderMaxMemory(maxDecompressedJSONBytes))
	})
	return zstdErr
}

// CompressJSON validates raw JSON, computes its SHA-256 hex digest, and returns zstd-compressed bytes.
func CompressJSON(raw []byte) ([]byte, string, error) {
	if !json.Valid(raw) {
		return nil, "", errors.New("invalid JSON payload")
	}
	if err := initZstd(); err != nil {
		return nil, "", fmt.Errorf("init zstd codec: %w", err)
	}
	sum := sha256.Sum256(raw)
	compressed := zstdEnc.EncodeAll(raw, make([]byte, 0, len(raw)/2))
	return compressed, hex.EncodeToString(sum[:]), nil
}

// DecompressJSON decodes zstd bytes and validates the resulting JSON payload.
func DecompressJSON(compressed []byte) (json.RawMessage, error) {
	if len(compressed) == 0 {
		return nil, errors.New("empty zstd payload")
	}
	if err := initZstd(); err != nil {
		return nil, fmt.Errorf("init zstd codec: %w", err)
	}
	decoded, err := zstdDec.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress zstd JSON: %w", err)
	}
	if len(decoded) > maxDecompressedJSONBytes {
		return nil, fmt.Errorf("decompressed JSON exceeds %d bytes", maxDecompressedJSONBytes)
	}
	if !json.Valid(decoded) {
		return nil, errors.New("decompressed payload is not valid JSON")
	}
	return json.RawMessage(decoded), nil
}
