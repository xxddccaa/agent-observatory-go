//go:build !windows

package discovery

import (
	"os"
	"syscall"
)

func statDevIno(info os.FileInfo) (uint64, uint64, bool) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st == nil {
		return 0, 0, false
	}
	return uint64(st.Dev), st.Ino, true
}
