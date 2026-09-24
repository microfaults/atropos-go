//go:build !linux

package proxy

import "net"

func platformOriginalDst(net.Conn) (string, error) { return "", ErrOriginalDstUnsupported }
