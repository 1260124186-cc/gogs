package git

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// slowCommand is a git command that runs for longer than the tests wait.
var slowCommand = []string{"-c", "alias.slow=!sleep 2", "slow"}

func TestExec_ContextTimeout(t *testing.T) {
	t.Run("context already expired before start", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		time.Sleep(time.Millisecond) // ensure deadline has passed
		_, err := exec(ctx, "", []string{"version"}, nil)
		assert.Equal(t, ErrExecTimeout, err)
	})

	t.Run("context deadline fires mid-execution", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := exec(ctx, "", slowCommand, nil)
		assert.Equal(t, ErrExecTimeout, err)
	})
}

func TestExec_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := exec(ctx, "", slowCommand, nil)
	assert.ErrorIs(t, err, context.Canceled)
	// Must NOT be ErrExecTimeout, cancellation is distinct from deadline.
	assert.NotEqual(t, ErrExecTimeout, err)
}

func TestWithTimeout(t *testing.T) {
	t.Run("zero uses the default timeout", func(t *testing.T) {
		ctx, cancel := WithTimeout(context.Background(), 0)
		defer cancel()
		_, ok := ctx.Deadline()
		assert.False(t, ok)
		_, err := exec(ctx, "", []string{"version"}, nil)
		assert.NoError(t, err)
	})

	t.Run("negative means no timeout", func(t *testing.T) {
		ctx, cancel := WithTimeout(context.Background(), -time.Second)
		defer cancel()
		deadline, ok := ctx.Deadline()
		assert.True(t, ok)
		assert.True(t, deadline.After(time.Now().AddDate(1, 0, 0)))
		_, err := exec(ctx, "", []string{"version"}, nil)
		assert.NoError(t, err)
	})

	t.Run("positive", func(t *testing.T) {
		ctx, cancel := WithTimeout(context.Background(), time.Minute)
		defer cancel()
		deadline, ok := ctx.Deadline()
		assert.True(t, ok)
		assert.WithinDuration(t, time.Now().Add(time.Minute), deadline, 5*time.Second)
	})
}

func TestExec_DefaultTimeoutApplied(t *testing.T) {
	// A plain context.Background() has no deadline. The command should still succeed
	// because DefaultTimeout is applied automatically and "git version" completes
	// well within that.
	ctx := context.Background()
	stdout, err := exec(ctx, "", []string{"version"}, nil)
	assert.NoError(t, err)
	assert.Contains(t, string(stdout), "git version")
}
