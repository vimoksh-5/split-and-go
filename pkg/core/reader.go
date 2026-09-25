package core

import "io"

// PatternReader generates arbitrary repetitive byte streams with zero memory allocation.
// Ideal for generating 10KB, 100MB, 1GB, 5GB, or 10GB streams on the fly without RAM impact.
type PatternReader struct {
	pattern []byte
	total   int64
	read    int64
}

// NewPatternReader creates an io.Reader producing up to total bytes by repeating pattern.
func NewPatternReader(pattern []byte, total int64) *PatternReader {
	if len(pattern) == 0 {
		pattern = []byte("SPLIT-AND-GO")
	}
	return &PatternReader{
		pattern: pattern,
		total:   total,
	}
}

// Read fills b with repeating pattern bytes up to the configured total limit.
func (p *PatternReader) Read(b []byte) (int, error) {
	if p.read >= p.total {
		return 0, io.EOF
	}
	remaining := p.total - p.read
	toRead := int64(len(b))
	if toRead > remaining {
		toRead = remaining
	}

	plen := int64(len(p.pattern))
	for i := int64(0); i < toRead; i++ {
		b[i] = p.pattern[(p.read+i)%plen]
	}
	p.read += toRead
	return int(toRead), nil
}
