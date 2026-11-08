package git

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepository_MergeBase(t *testing.T) {
	ctx := context.Background()

	t.Run("bad revision", func(t *testing.T) {
		mb, err := testrepo.MergeBase(ctx, "0eedd79eba4394bbef888c804e899731644367fe", "bad_revision")
		assert.True(t, IsErrNoMergeBase(err), "got %v", err)
		assert.Empty(t, mb)
	})

	t.Run("not a commit", func(t *testing.T) {
		mb, err := testrepo.MergeBase(ctx, "0eedd79eba4394bbef888c804e899731644367fe^{tree}", "master")
		assert.True(t, IsErrNoMergeBase(err), "got %v", err)
		assert.Empty(t, mb)
	})

	tests := []struct {
		base          string
		head          string
		wantMergeBase string
	}{
		{
			base:          "4e59b72440188e7c2578299fc28ea425fbe9aece",
			head:          "0eedd79eba4394bbef888c804e899731644367fe",
			wantMergeBase: "4e59b72440188e7c2578299fc28ea425fbe9aece",
		},
		{
			base:          "master",
			head:          "release-1.0",
			wantMergeBase: "0eedd79eba4394bbef888c804e899731644367fe",
		},
	}
	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			mb, err := testrepo.MergeBase(ctx, test.base, test.head)
			require.NoError(t, err)
			assert.Equal(t, test.wantMergeBase, mb)
		})
	}
}

func TestRepository_PullRequestMeta(t *testing.T) {
	ctx := context.Background()

	base, cleanupBase, err := setupTempRepo()
	require.NoError(t, err)
	defer cleanupBase()
	head, cleanupHead, err := setupTempRepo()
	require.NoError(t, err)
	defer cleanupHead()

	mergeBase, err := base.RevParse(ctx, "master")
	require.NoError(t, err)

	// Two commits that change three files on the head branch, plus a commit on
	// the base branch that must not be counted.
	createBranch(t, head, "feature")
	commitFiles(t, head, "feature", map[string]string{"a.txt": "a", "b.txt": "b"})
	commitFiles(t, head, "feature", map[string]string{"c.txt": "c"})
	commitFiles(t, base, "master", map[string]string{"base.txt": "base"})

	t.Run("fork", func(t *testing.T) {
		meta, err := head.PullRequestMeta(ctx, base.Path(), "feature", "master")
		require.NoError(t, err)
		assert.Equal(t, mergeBase, meta.MergeBase)
		require.Len(t, meta.Commits, 2)
		assert.Equal(t, "Update c.txt", meta.Commits[0].Summary())
		assert.Equal(t, 3, meta.NumFiles)

		remotes, err := exec(ctx, head.Path(), []string{"remote"}, nil)
		require.NoError(t, err)
		assert.Equal(t, "origin\n", string(remotes), "temporary remote should be removed")
	})

	t.Run("same repository", func(t *testing.T) {
		meta, err := head.PullRequestMeta(ctx, head.Path(), "feature", "master")
		require.NoError(t, err)
		assert.Equal(t, mergeBase, meta.MergeBase)
		assert.Len(t, meta.Commits, 2)
		assert.Equal(t, 3, meta.NumFiles)
	})

	t.Run("no merge base", func(t *testing.T) {
		commitFiles(t, head, "lonely", map[string]string{"lonely.txt": "lonely"})

		_, err := head.PullRequestMeta(ctx, base.Path(), "lonely", "master")
		assert.True(t, IsErrNoMergeBase(err), "got %v", err)
	})
}
