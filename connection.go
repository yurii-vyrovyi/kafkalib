package kafkalib

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/hashicorp/go-multierror"
)

// checkConnections is a helper function for a ping function.
// It makes attempts to connect to a counterparty until pingFunc will succeed or a timeout will be exceeded.
// Delay between attempts increases as a power 2 (sec).
// This allows to make a safe connection check in case if a counterparty is not ready immediately after service start.
func checkConnections(ctx context.Context, pingFunc func() error, timeout time.Duration, logger Logger) error {

	retChan := make(chan error)

	go func() {
		defer close(retChan)

		attempts := 0
		delaySecs := int64(0)

		now := time.Now().UTC()

		for {

			select {

			case <-ctx.Done():
				return

			case <-time.After(time.Duration(delaySecs) * time.Second):

				var retErr error

				if err := pingFunc(); err != nil {
					retErr = multierror.Append(retErr, err)
				}

				if retErr == nil {
					return
				}

				logger.Infof("connection attempt %d: %v", attempts, retErr)

				delaySecs = int64(math.Pow(2, float64(attempts)))
				attempts++

				if time.Since(now) > timeout {
					retChan <- fmt.Errorf("connection timeout: %w", retErr)
					return
				}
			}

		}
	}()

	return <-retChan
}
