package rangepool

import (
	"github.com/giantswarm/microerror"
)

var capacityReachedError = &microerror.Error{
	Kind: "capacityReachedError",
}

// IsCapacityReached asserts capacityReachedError.
func IsCapacityReached(err error) bool {
	return microerror.Cause(err) == capacityReachedError
}

var executionFailedError = &microerror.Error{
	Kind: "executionFailed",
}

// IsExecutionFailed asserts executionFailedError.
func IsExecutionFailed(err error) bool {
	return microerror.Cause(err) == executionFailedError
}

var invalidConfigError = &microerror.Error{
	Kind: "invalidConfigError",
}

// IsInvalidConfig asserts invalidConfigError.
func IsInvalidConfig(err error) bool {
	return microerror.Cause(err) == invalidConfigError
}

var itemsNotFoundError = &microerror.Error{
	Kind: "itemsNotFoundError",
}

// IsItemsNotFound asserts itemsNotFoundError.
func IsItemsNotFound(err error) bool {
	return microerror.Cause(err) == itemsNotFoundError
}
