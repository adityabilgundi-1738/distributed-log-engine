package mapreduce

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
)

var defaultEndpoints = []string{
	"/api/v1/search",
	"/api/v1/checkout",
	"/api/v1/inventory",
	"/api/v1/recommendations",
	"/api/v1/auth/token",
	"/api/v1/orders/status",
	"/api/v1/payments/charge",
	"/api/v1/telemetry/ingest",
}

var defaultMethods = []string{"GET", "POST", "PUT"}
var defaultStatusCodes = []int{200, 200, 200, 200, 201, 200, 204, 400, 404, 500, 503}

func GenerateSyntheticLogFile(path string, numLines int, seed int64) (int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 1<<20)
	rng := rand.New(rand.NewSource(seed))
	var totalBytes int64
	var lineBuf []byte

	for i := 0; i < numLines; i++ {
		ep := defaultEndpoints[rng.Intn(len(defaultEndpoints))]
		method := defaultMethods[rng.Intn(len(defaultMethods))]
		status := defaultStatusCodes[rng.Intn(len(defaultStatusCodes))]
		latencyMicros := 250 + rng.Intn(45000)
		if status >= 500 {
			latencyMicros += 25000
		}
		bytesSent := 128 + rng.Intn(16384)

		lineBuf = lineBuf[:0]
		lineBuf = append(lineBuf, "2026-10-25T19:00:00Z "...)
		lineBuf = append(lineBuf, method...)
		lineBuf = append(lineBuf, ' ')
		lineBuf = append(lineBuf, ep...)
		lineBuf = append(lineBuf, ' ')
		lineBuf = strconv.AppendInt(lineBuf, int64(status), 10)
		lineBuf = append(lineBuf, ' ')
		lineBuf = strconv.AppendInt(lineBuf, int64(latencyMicros), 10)
		lineBuf = append(lineBuf, ' ')
		lineBuf = strconv.AppendInt(lineBuf, int64(bytesSent), 10)
		lineBuf = append(lineBuf, " trace_id=req-"...)
		lineBuf = strconv.AppendInt(lineBuf, int64(i), 10)
		lineBuf = append(lineBuf, '\n')

		n, err := w.Write(lineBuf)
		if err != nil {
			return totalBytes, err
		}
		totalBytes += int64(n)
	}

	if err := w.Flush(); err != nil {
		return totalBytes, err
	}
	return totalBytes, nil
}

func SplitFileIntoChunks(path string, targetChunkBytes int64) ([]ChunkSpec, error) {
	if targetChunkBytes <= 0 {
		return nil, fmt.Errorf("targetChunkBytes must be positive")
	}

	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()
	if fileSize == 0 {
		return nil, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var chunks []ChunkSpec
	var offset int64
	chunkID := 0
	scanBuf := make([]byte, 4096)

	for offset < fileSize {
		tentativeEnd := offset + targetChunkBytes
		if tentativeEnd >= fileSize {
			chunks = append(chunks, ChunkSpec{
				ID:       chunkID,
				FilePath: path,
				Offset:   offset,
				Length:   fileSize - offset,
			})
			break
		}

		if _, err := f.Seek(tentativeEnd, io.SeekStart); err != nil {
			return nil, err
		}

		var extra int64
		foundNewline := false
		for !foundNewline {
			n, readErr := f.Read(scanBuf)
			for i := 0; i < n; i++ {
				extra++
				if scanBuf[i] == '\n' {
					foundNewline = true
					break
				}
			}
			if foundNewline || readErr == io.EOF {
				break
			}
			if readErr != nil {
				return nil, readErr
			}
		}

		chunkEnd := tentativeEnd + extra
		if chunkEnd > fileSize {
			chunkEnd = fileSize
		}
		chunks = append(chunks, ChunkSpec{
			ID:       chunkID,
			FilePath: path,
			Offset:   offset,
			Length:   chunkEnd - offset,
		})
		offset = chunkEnd
		chunkID++
	}

	return chunks, nil
}
