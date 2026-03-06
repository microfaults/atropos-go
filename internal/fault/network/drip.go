package network

import (
	"context"
	"io"
	"time"
)

// Drip writes data in tiny chunks with pauses between them,
// simulating an extremely slow connection.
//
// A 10KB response with ChunkSize=100 and Interval=100ms takes ~10s.
// Models: degraded last-mile, overloaded proxy, server trickling output.
type Drip struct {
	// ChunkSize is bytes per drip. Default: 1.
	ChunkSize int

	// Interval is the pause between each chunk write.
	Interval time.Duration
}

func (d *Drip) Pipe(ctx context.Context, src io.Reader, dst io.Writer) error {
	chunk := d.ChunkSize
	if chunk <= 0 {
		chunk = 1
	}

	buf := make([]byte, 32*1024)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, readErr := src.Read(buf)
		if n > 0 {
			data := buf[:n]
			for len(data) > 0 {
				size := chunk
				if size > len(data) {
					size = len(data)
				}

				if _, err := dst.Write(data[:size]); err != nil {
					return err
				}
				data = data[size:]

				if len(data) > 0 && d.Interval > 0 {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(d.Interval):
					}
				}
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return nil
			}
			return readErr
		}
	}
}
