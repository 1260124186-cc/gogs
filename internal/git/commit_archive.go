package git

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
)

// ArchiveFormat is the format of an archive.
type ArchiveFormat string

// A list of formats can be created by Git for an archive.
const (
	ArchiveZip   ArchiveFormat = "zip"
	ArchiveTarGz ArchiveFormat = "tar.gz"
)

// Archive creates given format of archive to the destination. The destination
// only ever holds a complete archive, because the archive is written to a
// temporary file next to it and renamed into place once Git succeeds.
func (c *Commit) Archive(ctx context.Context, format ArchiveFormat, dst string) error {
	tmp, err := os.CreateTemp(filepath.Dir(dst), filepath.Base(dst)+".tmp-*")
	if err != nil {
		return errors.Wrap(err, "create temporary file")
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	err = tmp.Chmod(0o644)
	_ = tmp.Close()
	if err != nil {
		return errors.Wrap(err, "change mode of temporary file")
	}

	prefix := filepath.Base(strings.TrimSuffix(c.repo.path, ".git")) + "/"
	_, err = exec(ctx,
		c.repo.path,
		[]string{
			"archive",
			"--prefix=" + prefix,
			"--format=" + string(format),
			"-o", tmpPath,
			"--end-of-options",
			c.ID.String(),
		},
		nil,
	)
	if err != nil {
		return err
	}

	err = os.Rename(tmpPath, dst)
	if err != nil {
		// On Windows, the rename fails while a concurrent request that created the
		// same archive is still serving it.
		if _, statErr := os.Stat(dst); statErr == nil {
			return nil
		}
		return errors.Wrap(err, "rename temporary file")
	}
	return nil
}
