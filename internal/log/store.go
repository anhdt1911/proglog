package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// Encode architectures, more here: https://www.techtarget.com/searchnetworking/definition/big-endian-and-little-endian.
	enc = binary.BigEndian
)

const (
	// lenWidth defines the length to store data length
	lenWidth = 8
)

type store struct {
	// Representation of a file.
	*os.File
	// For lock and unlock to avoid race condition.
	mu sync.Mutex
	// As the name say, it's a buffer writer.
	// Add a buffer to store data until full or flush to reduce system call but will increase write step.
	buf *bufio.Writer

	size uint64
}

func newStore(file *os.File) (*store, error) {
	fileInfo, err := os.Stat(file.Name())
	if err != nil {
		return nil, err
	}

	// Set the size to avoid unintentionally overwriting existing data.
	size := uint64(fileInfo.Size())

	return &store{
		File: file,
		size: size,
		buf:  bufio.NewWriter(file),
	}, nil
}

func (s *store) Append(data []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Current file size.
	pos = s.size

	// Write data length to the buffer.
	// So that when reading the data, we know how many bytes to read.
	if err := binary.Write(s.buf, enc, uint64(len(data))); err != nil {
		return 0, 0, err
	}
	// Write data to buffer.
	w, err := s.buf.Write(data)
	if err != nil {
		return 0, 0, err
	}

	// Increase file size.
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil

}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// To write any remaining data on the buffer to file.
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	// To get the size of data we want to read from.
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	// Convert to size to uint
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}