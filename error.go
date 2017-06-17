package rangepool

import (
	"github.com/juju/errgo"
)

var capacityReachedError = errgo.New("capacity reached")

// IsCapacityReached asserts capacityReachedError.
func IsCapacityReached(err error) bool {
	return errgo.Cause(err) == capacityReachedError
}

var executionFailedError = errgo.New("execution failed")

// IsExecutionFailed asserts executionFailedError.
func IsExecutionFailed(err error) bool {
	return errgo.Cause(err) == executionFailedError
}

var invalidConfigError = errgo.New("invalid config")

// IsInvalidConfig asserts invalidConfigError.
func IsInvalidConfig(err error) bool {
	return errgo.Cause(err) == invalidConfigError
}

var invalidDomainError = errgo.New("invalid domain")

// IsInvalidDomain asserts invalidDomainError.
func IsInvalidDomain(err error) bool {
	return errgo.Cause(err) == invalidDomainError
}
