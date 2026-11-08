package git

import (
	"bytes"
	"context"
	"strings"
)

// LsRemoteOptions contains arguments for listing references in a remote
// repository.
//
// Docs: https://git-scm.com/docs/git-ls-remote
type LsRemoteOptions struct {
	// Indicates whether include heads.
	Heads bool
	// Indicates whether include tags.
	Tags bool
	// Indicates whether to not show peeled tags or pseudo refs.
	Refs bool
	// The list of patterns to filter results.
	Patterns []string
	CommandOptions
}

// LsRemote returns a list of references in the remote repository.
func LsRemote(ctx context.Context, url string, opts ...LsRemoteOptions) ([]*Reference, error) {
	var opt LsRemoteOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	args := []string{"ls-remote", "--quiet"}
	if opt.Heads {
		args = append(args, "--heads")
	}
	if opt.Tags {
		args = append(args, "--tags")
	}
	if opt.Refs {
		args = append(args, "--refs")
	}
	args = append(args, "--end-of-options", url)
	if len(opt.Patterns) > 0 {
		args = append(args, opt.Patterns...)
	}

	stdout, err := exec(ctx, "", args, opt.Envs)
	if err != nil {
		return nil, err
	}

	lines := bytes.Split(stdout, []byte("\n"))
	refs := make([]*Reference, 0, len(lines))
	for i := range lines {
		fields := bytes.Fields(lines[i])
		if len(fields) < 2 {
			continue
		}

		refs = append(refs, &Reference{
			ID:      string(fields[0]),
			Refspec: string(fields[1]),
		})
	}
	return refs, nil
}

// RemoteAddOptions contains options to add a remote address.
//
// Docs: https://git-scm.com/docs/git-remote#Documentation/git-remote.txt-emaddem
type RemoteAddOptions struct {
	// Indicates whether to execute git fetch after the remote information is set
	// up.
	Fetch bool
	// Indicates whether to add remote as mirror with --mirror=fetch.
	MirrorFetch bool
	CommandOptions
}

// RemoteAdd adds a new remote to the repository.
func (r *Repository) RemoteAdd(ctx context.Context, name, url string, opts ...RemoteAddOptions) error {
	var opt RemoteAddOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	args := []string{"remote", "add"}
	if opt.Fetch {
		args = append(args, "-f")
	}
	if opt.MirrorFetch {
		args = append(args, "--mirror=fetch")
	}
	args = append(args, "--end-of-options", name, url)

	_, err := exec(ctx, r.path, args, opt.Envs)
	return err
}

// RemoteRemoveOptions contains arguments for removing a remote from the
// repository.
//
// Docs: https://git-scm.com/docs/git-remote#Documentation/git-remote.txt-emremoveem
type RemoteRemoveOptions struct {
	CommandOptions
}

// RemoteRemove removes a remote from the repository.
func (r *Repository) RemoteRemove(ctx context.Context, name string, opts ...RemoteRemoveOptions) error {
	var opt RemoteRemoveOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	args := []string{"remote", "remove", "--end-of-options", name}
	_, err := exec(ctx, r.path, args, opt.Envs)
	if err != nil {
		if strings.Contains(err.Error(), "error: No such remote") ||
			strings.Contains(err.Error(), "fatal: No such remote") {
			return ErrRemoteNotExist
		}
		return err
	}
	return nil
}

// RemoteUpdateOptions contains optional arguments for fetching updates from all
// remotes of the repository.
//
// Docs: https://git-scm.com/docs/git-remote#Documentation/git-remote.txt-update
type RemoteUpdateOptions struct {
	// Indicates whether to remove references that no longer exist on the remotes.
	Prune bool
	CommandOptions
}

// RemoteUpdate fetches updates from all remotes of the repository. It returns
// the standard error output, which is where Git reports updated references.
func (r *Repository) RemoteUpdate(ctx context.Context, opts ...RemoteUpdateOptions) (string, error) {
	var opt RemoteUpdateOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	args := []string{"remote", "update"}
	if opt.Prune {
		args = append(args, "--prune")
	}

	c, cancel := cmd(ctx, r.path, args, opt.Envs)
	defer cancel()

	stderr := new(bytes.Buffer)
	if err := c.StdErr().Run().Stream(stderr); err != nil {
		return "", mapContextError(err, ctx)
	}
	return stderr.String(), nil
}
