package git

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"

	clog "unknwon.dev/clog/v2"
)

// MergeBase returns merge base between base and head revisions of the
// repository.
func (r *Repository) MergeBase(ctx context.Context, base, head string) (string, error) {
	args := []string{"merge-base", "--end-of-options", base, head}
	stdout, err := exec(ctx, r.path, args, nil)
	if err != nil {
		if isExitStatus(err, 1) {
			return "", ErrNoMergeBase
		}
		return "", err
	}
	return strings.TrimSpace(string(stdout)), nil
}

// PullRequestMeta contains metadata for a pull request.
type PullRequestMeta struct {
	// The merge base of the pull request.
	MergeBase string
	// The commits that are requested to be merged.
	Commits []*Commit
	// The number of files changed.
	NumFiles int
}

// PullRequestMeta gathers metadata of a pull request that merges the head
// branch of the repository into the base branch of the repository at the
// basePath, which may be the same repository.
func (r *Repository) PullRequestMeta(ctx context.Context, basePath, headBranch, baseBranch string) (*PullRequestMeta, error) {
	tmpRemoteBranch := baseBranch

	// We need to create a temporary remote when the pull request is sent from a forked repository.
	if r.path != basePath {
		// Adding and removing the temporary remote must not be cut short when ctx is
		// canceled, e.g., by a client disconnecting, or it is left behind with all of
		// the fetched references.
		remoteCtx := context.WithoutCancel(ctx)
		tmpRemote := strconv.FormatInt(time.Now().UnixNano(), 10)
		defer func() {
			err := r.RemoteRemove(remoteCtx, tmpRemote)
			if err != nil && !errors.Is(err, ErrRemoteNotExist) {
				clog.Error("Failed to remove remote %q [path: %s]: %v", tmpRemote, r.path, err)
			}
		}()
		err := r.RemoteAdd(remoteCtx, tmpRemote, basePath, RemoteAddOptions{Fetch: true})
		if err != nil {
			return nil, errors.Wrap(err, "add remote")
		}

		tmpRemoteBranch = "remotes/" + tmpRemote + "/" + baseBranch
	}

	mergeBase, err := r.MergeBase(ctx, tmpRemoteBranch, headBranch)
	if err != nil {
		return nil, errors.Wrap(err, "get merge base")
	}

	commits, err := r.Log(ctx, mergeBase+"..."+headBranch)
	if err != nil {
		return nil, errors.Wrap(err, "get commits")
	}

	// Count number of changed files
	names, err := r.DiffNameOnly(ctx, tmpRemoteBranch, headBranch, DiffNameOnlyOptions{NeedsMergeBase: true})
	if err != nil {
		return nil, errors.Wrap(err, "get changed files")
	}

	return &PullRequestMeta{
		MergeBase: mergeBase,
		Commits:   commits,
		NumFiles:  len(names),
	}, nil
}
