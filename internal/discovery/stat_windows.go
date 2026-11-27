//go:build windows

package discovery

import "os"

func statDevIno(_ os.FileInfo) (uint64, uint64, bool) {
	return 0, 0, false
}
