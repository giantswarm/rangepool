package rangepool

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	microerror "github.com/giantswarm/microkit/error"
	micrologger "github.com/giantswarm/microkit/logger"
	microstorage "github.com/giantswarm/microkit/storage"
)

const (
	// IDKeyFormat is the format string used to create a storage key to persist
	// the relationship between IDs and items.
	//
	//     range-pool/${namespace1}/id/${id1}/item/${item1}    ${item1}
	//     range-pool/${namespace1}/id/${id1}/item/${item2}    ${item2}
	//     range-pool/${namespace1}/id/${id2}/item/${item3}    ${item3}
	//     range-pool/${namespace1}/id/${id2}/item/${item4}    ${item4}
	//
	IDKeyFormat = "range-pool/%s/id/%s/item/%s"
	// IDListKeyFormat is the format string used to create a storage key to lookup
	// the list of items of an ID. See also IDKeyFormat.
	IDListKeyFormat = "range-pool/%s/id/%s/item"
	// ItemKeyFormat is the format string used to create a storage key to persist
	// the relation between a namespace and its associated items.
	//
	//     range-pool/${namespace1}/item/${item1}    ${item1}
	//     range-pool/${namespace1}/item/${item2}    ${item2}
	//     range-pool/${namespace1}/item/${item3}    ${item3}
	//     range-pool/${namespace1}/item/${item4}    ${item4}
	//
	ItemKeyFormat = "range-pool/%s/item/%s"
	// ItemListKeyFormat is the format string used to create a storage key to
	// lookup the list of items of a namespace. See also ItemKeyFormat.
	ItemListKeyFormat = "range-pool/%s/item"
	// LatestKeyFormat is used to create a storage key to persist the latest item
	// used.
	//
	//     range-pool/${namespace1}/latest    ${item4}
	//
	LatestKeyFormat = "range-pool/%s/latest"
)

const (
	// latestItemException indicates there was no latest range pool item, which
	// means there has never been an item before. In this case the range pool is
	// completely new and about to be used the very first time.
	latestItemException = -1
)

// Config represents the configuration used to create a new range pool.
type Config struct {
	// Dependencies.
	Logger  micrologger.Logger
	Storage microstorage.Service
}

// DefaultConfig provides a default configuration to create a new range pool by
// best effort.
func DefaultConfig() Config {
	var err error

	var newLogger micrologger.Logger
	{
		config := micrologger.DefaultConfig()
		newLogger, err = micrologger.New(config)
		if err != nil {
			panic(err)
		}
	}

	var newStorage microstorage.Service
	{
		config := microstorage.DefaultConfig()
		newStorage, err = microstorage.New(config)
		if err != nil {
			panic(err)
		}
	}

	return Config{
		// Dependencies.
		Logger:  newLogger,
		Storage: newStorage,
	}
}

// New creates a new configured range pool.
func New(config Config) (*Service, error) {
	// Dependencies.
	if config.Logger == nil {
		return nil, microerror.MaskAnyf(invalidConfigError, "logger must not be empty")
	}
	if config.Storage == nil {
		return nil, microerror.MaskAnyf(invalidConfigError, "storage must not be empty")
	}

	newService := &Service{
		// Dependencies.
		logger:  config.Logger,
		storage: config.Storage,
	}

	return newService, nil
}

type Service struct {
	// Dependencies.
	logger  micrologger.Logger
	storage microstorage.Service
}

func (s *Service) Create(ctx context.Context, namespace, ID string, num, min, max int) ([]int, error) {
	var err error

	// Fetch a list of items we already created. Here we receive a list of items
	// that may or may not have gaps in it. In case some items have been deleted
	// there might be gaps, because items are freed and removed from the list.
	var used []int
	{
		v, err := s.storage.List(ctx, fmt.Sprintf(ItemListKeyFormat, namespace))
		if microstorage.IsNotFound(err) {
			// In case there is no item yet, we create and persist the first ones
			// using the algorithm invoked below.
		} else if err != nil {
			return nil, microerror.MaskAny(err)
		}
		used, err = stringsToInts(v)
		if err != nil {
			return nil, microerror.MaskAny(err)
		}
	}

	// Fetch the latest item used.
	var latest int
	{
		l, err := s.storage.Search(ctx, fmt.Sprintf(LatestKeyFormat, namespace))
		if microstorage.IsNotFound(err) {
			// In case there is no latest item yet, we set it to the special case -1.
			// This indicates the first item for the algorithm being invoked below.
			l = strconv.Itoa(latestItemException)
		} else if err != nil {
			return nil, microerror.MaskAny(err)
		}

		latest, err = strconv.Atoi(l)
		if err != nil {
			return nil, microerror.MaskAny(err)
		}
	}

	// Find and persist the next items.
	var items []int
	{
		for i := 0; i < num; i++ {
			item, err := nextItem(used, min, max, latest)
			if err != nil {
				return nil, microerror.MaskAny(err)
			}
			items = append(items, item)
			used = append(used, item)
		}

		err = s.create(ctx, namespace, ID, items)
		if err != nil {
			return nil, microerror.MaskAny(err)
		}
	}

	return items, nil
}

func (s *Service) Delete(ctx context.Context, namespace, ID string) error {
	var items []int
	{
		v, err := s.storage.List(ctx, fmt.Sprintf(IDListKeyFormat, namespace, ID))
		if microstorage.IsNotFound(err) {
			// In case there is no item yet, we create and persist the first ones
			// using the algorithm invoked below.
		} else if err != nil {
			return microerror.MaskAny(err)
		}
		items, err = stringsToInts(v)
		if err != nil {
			return microerror.MaskAny(err)
		}
	}

	err := s.delete(ctx, namespace, ID, items)
	if err != nil {
		return microerror.MaskAny(err)
	}

	return nil
}

// create is used to persist new items.
func (s *Service) create(ctx context.Context, namespace, ID string, items []int) error {
	for _, item := range items {
		i := strconv.Itoa(item)

		// We store the relationship between the namespace and its corresponding
		// item to be able to list all of the items later.
		err := s.storage.Create(ctx, fmt.Sprintf(ItemKeyFormat, namespace, i), i)
		if err != nil {
			return microerror.MaskAny(err)
		}
		// We store the relationship between the ID and its corresponding item to be
		// able to delete it later based on the ID.
		err = s.storage.Create(ctx, fmt.Sprintf(IDKeyFormat, namespace, ID, i), i)
		if err != nil {
			return microerror.MaskAny(err)
		}
	}

	// We store the latest item to have a pointer from which we can derive the
	// next item to use.
	lastItem := strconv.Itoa(items[len(items)-1])
	err := s.storage.Create(ctx, fmt.Sprintf(LatestKeyFormat, namespace), lastItem)
	if err != nil {
		return microerror.MaskAny(err)
	}

	return nil
}

func (s *Service) delete(ctx context.Context, namespace, ID string, items []int) error {
	for _, item := range items {
		i := strconv.Itoa(item)

		err := s.storage.Delete(ctx, fmt.Sprintf(ItemKeyFormat, namespace, i))
		if err != nil {
			return microerror.MaskAny(err)
		}
		err = s.storage.Delete(ctx, fmt.Sprintf(IDKeyFormat, namespace, ID, i))
		if err != nil {
			return microerror.MaskAny(err)
		}
	}

	err := s.storage.Delete(ctx, fmt.Sprintf(IDListKeyFormat, namespace, ID))
	if err != nil {
		return microerror.MaskAny(err)
	}

	list, err := s.storage.List(ctx, fmt.Sprintf(ItemListKeyFormat, namespace))
	if microstorage.IsNotFound(err) {
		// In case there is no item anymore, we just go ahead to delete the complete
		// item list key and latest item key.
	} else if err != nil {
		return microerror.MaskAny(err)
	}
	if len(list) == 0 {
		err := s.storage.Delete(ctx, fmt.Sprintf(ItemListKeyFormat, namespace))
		if err != nil {
			return microerror.MaskAny(err)
		}
		err = s.storage.Delete(ctx, fmt.Sprintf(LatestKeyFormat, namespace))
		if err != nil {
			return microerror.MaskAny(err)
		}
	}

	return nil
}

// nextItem implements a stateless algorithm to sort out the next item to use.
// The first parameter used defines the items already in use. These cannot be
// taken again, because they have to be unique by protocol. min and max
// represent the configured range pool boundaries. No items outside of their
// range must be used. min and max must not be negative. latest represents the
// latest item being used. It is used make up the next item in the series by
// incrementing it by 1. latest is special because it can be -1, which means
// there is no latest known item already, which implies the very first item
// being created by the range pool.
func nextItem(used []int, min, max, latest int) (int, error) {
	if min <= -1 {
		return 0, microerror.MaskAnyf(executionFailedError, "min must be negative")
	}
	if max <= -1 {
		return 0, microerror.MaskAnyf(executionFailedError, "max must be negative")
	}
	if min >= max {
		return 0, microerror.MaskAnyf(executionFailedError, "min must be greater than max")
	}
	if latest != latestItemException && latest < min {
		return 0, microerror.MaskAnyf(executionFailedError, "latest must not be lower than min")
	}
	if latest != latestItemException && latest > max {
		return 0, microerror.MaskAnyf(executionFailedError, "latest must not be greater than max")
	}

	sort.Ints(used)

	iterator := func(min, max int) int {
		for i := min; i <= max; i++ {
			// Ignore the items being used already.
			if containsInt(used, i) {
				continue
			}

			return i
		}

		// We couldn't find any item in the given range.
		return latestItemException
	}

	var nextItem int

	if latest != latestItemException {
		nextItem = iterator(latest+1, max)
		if nextItem != latestItemException {
			return nextItem, nil
		}
	}

	nextItem = iterator(min, max)
	if nextItem != latestItemException {
		return nextItem, nil
	}

	return 0, microerror.MaskAnyf(capacityReachedError, "cannot find next item")
}

func containsInt(list []int, item int) bool {
	for _, l := range list {
		if l == item {
			return true
		}
	}

	return false
}

// stringsToInts takes a list of strings and returns the equivalent list of
// ints.
func stringsToInts(list []string) ([]int, error) {
	var converted []int

	for _, l := range list {
		s, err := strconv.Atoi(l)
		if err != nil {
			return nil, microerror.MaskAny(err)
		}

		converted = append(converted, s)
	}

	return converted, nil
}
