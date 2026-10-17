package relay

import "os"

// writeFileAtomic menulis data ke path lewat file temporer lalu rename, supaya
// pembaca tidak pernah melihat file yang setengah tertulis.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, perm); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
