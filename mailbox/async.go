package mailbox

import (
	"context"
	"log/slog"
)

// AsyncRunner is the narrow host lifecycle seam used for asynchronous
// wake reactions. Implementations may track and drain work during shutdown.
type AsyncRunner interface {
	Go(name string, fn func(context.Context))
}

// goSafe starts untracked fallback work while containing panics. Service
// owners that need shutdown draining should configure an AsyncRunner.
func goSafe(ctx context.Context, name string, fn func(context.Context)) {
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				slog.Error("mailbox: asynchronous callback panicked",
					"name", name, "panic", recovered)
			}
		}()
		fn(ctx)
	}()
}
