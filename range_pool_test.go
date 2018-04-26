package rangepool

import (
	"context"
	"testing"

	"github.com/giantswarm/micrologger/microloggertest"
	"github.com/giantswarm/microstorage"
	"github.com/giantswarm/microstorage/memory"
)

func Test_Service_Create_NumOne(t *testing.T) {
	// Create a new storage and service.
	var err error
	var newService *Service
	var newStorage microstorage.Storage
	{
		newStorage, err = memory.New(memory.DefaultConfig())
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		config := DefaultConfig()
		config.Logger = microloggertest.New()
		config.Storage = newStorage
		newService, err = New(config)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}
	}

	// Prepare the test variables.
	ctx := context.TODO()
	namespace := "test-namespace"
	ID := "test-id"
	num := 1
	min := 2
	max := 9

	// Execute and assert the actually tested functionality. At first we fetch a
	// new item.
	{
		items, err := newService.Create(ctx, namespace, ID, num, min, max)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		l := len(items)
		if l != 1 {
			t.Fatal("expected", 1, "got", l)
		}

		i1 := items[0]
		if i1 != 2 {
			t.Fatal("expected", 2, "got", i1)
		}
	}

	// Fetch another item.
	{
		items, err := newService.Create(ctx, namespace, ID, num, min, max)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		l := len(items)
		if l != 1 {
			t.Fatal("expected", 1, "got", l)
		}

		i1 := items[0]
		if i1 != 3 {
			t.Fatal("expected", 3, "got", i1)
		}
	}

	// Delete the namespaced items for the test ID.
	{
		err := newService.Delete(ctx, namespace, ID)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}
	}

	// Fetch a new item. This should be based on the last item we created. So the
	// last item was 3, which implies the item we expect now is 4. We do not want
	// to purge the last item pointer to rotate through the available options all
	// the time to be more efficient in certain edge cases.
	{
		items, err := newService.Create(ctx, namespace, ID, num, min, max)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		l := len(items)
		if l != 1 {
			t.Fatal("expected", 1, "got", l)
		}

		i1 := items[0]
		if i1 != 4 {
			t.Fatal("expected", 4, "got", i1)
		}
	}
}

func Test_Service_Create_Num3_CapacityReached(t *testing.T) {
	// Create a new storage and service.
	var err error
	var newService *Service
	var newStorage microstorage.Storage
	{
		newStorage, err = memory.New(memory.DefaultConfig())
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		config := DefaultConfig()
		config.Logger = microloggertest.New()
		config.Storage = newStorage
		newService, err = New(config)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}
	}

	// Prepare the test variables.
	ctx := context.TODO()
	namespace := "test-namespace"
	ID := "test-id"
	num := 3
	min := 2
	max := 7

	// Execute and assert the actually tested functionality. At first we fetch the
	// new items.
	{
		items, err := newService.Create(ctx, namespace, ID, num, min, max)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		l := len(items)
		if l != 3 {
			t.Fatal("expected", 3, "got", l)
		}
	}

	// Fetch items again. This should saturate our configured capacity.
	{
		items, err := newService.Create(ctx, namespace, ID, num, min, max)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		l := len(items)
		if l != 3 {
			t.Fatal("expected", 3, "got", l)
		}
	}

	// Fetch new items again. This should throw an error since the capacity of
	// available items should be reached.
	{
		_, err := newService.Create(ctx, namespace, ID, num, min, max)
		if !IsCapacityReached(err) {
			t.Fatal("expected", true, "got", false)
		}
	}
}

func Test_Service_Create_NumThree_Rotate(t *testing.T) {
	// Create a new storage and service.
	var newService *Service
	{
		newStorage, err := memory.New(memory.DefaultConfig())
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		config := DefaultConfig()
		config.Logger = microloggertest.New()
		config.Storage = newStorage
		newService, err = New(config)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}
	}

	// Prepare the test variables.
	ctx := context.TODO()
	namespace := "test-namespace"
	num := 3
	min := 2
	max := 7

	// We start at the minimum boundary and allocate 2, 3 and 4 for the first ID.
	{
		ID := "test-id-1"
		items, err := newService.Create(ctx, namespace, ID, num, min, max)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		l := len(items)
		if l != 3 {
			t.Fatal("expected", 3, "got", l)
		}

		i1 := items[0]
		if i1 != 2 {
			t.Fatal("expected", 2, "got", i1)
		}
		i2 := items[1]
		if i2 != 3 {
			t.Fatal("expected", 3, "got", i2)
		}
		i3 := items[2]
		if i3 != 4 {
			t.Fatal("expected", 4, "got", i3)
		}
	}

	// We continue with the last known pointer and allocate 5, 6 and 7 for the
	// second ID. Now we also reached the max boundary. In case any items are
	// still free we expect rangepool to rotate and start from the minimum
	// boundary again.
	{
		ID := "test-id-2"
		items, err := newService.Create(ctx, namespace, ID, num, min, max)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		l := len(items)
		if l != 3 {
			t.Fatal("expected", 3, "got", l)
		}

		i1 := items[0]
		if i1 != 5 {
			t.Fatal("expected", 5, "got", i1)
		}
		i2 := items[1]
		if i2 != 6 {
			t.Fatal("expected", 6, "got", i2)
		}
		i3 := items[2]
		if i3 != 7 {
			t.Fatal("expected", 7, "got", i3)
		}
	}

	// We delete the allocated items of the first ID to make rangepool rotate and
	// start allocating from the minimum boundary.
	{
		ID := "test-id-1"
		err := newService.Delete(ctx, namespace, ID)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}
	}

	// We expect item allocations starting from the minimum boundaries because we
	// filled the stack and freed the start of the available items in the pool.
	{
		ID := "test-id-3"
		items, err := newService.Create(ctx, namespace, ID, num, min, max)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		l := len(items)
		if l != 3 {
			t.Fatal("expected", 3, "got", l)
		}

		i1 := items[0]
		if i1 != 2 {
			t.Fatal("expected", 2, "got", i1)
		}
		i2 := items[1]
		if i2 != 3 {
			t.Fatal("expected", 3, "got", i2)
		}
		i3 := items[2]
		if i3 != 4 {
			t.Fatal("expected", 4, "got", i3)
		}
	}
}

func Test_Service_Create_NumTwo(t *testing.T) {
	// Create a new storage and service.
	var err error
	var newService *Service
	var newStorage microstorage.Storage
	{
		newStorage, err = memory.New(memory.DefaultConfig())
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		config := DefaultConfig()
		config.Logger = microloggertest.New()
		config.Storage = newStorage
		newService, err = New(config)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}
	}

	// Prepare the test variables.
	ctx := context.TODO()
	namespace := "test-namespace"
	ID := "test-id"
	num := 2
	min := 2
	max := 9

	// Execute and assert the actually tested functionality. At first we fetch the
	// new items.
	{
		items, err := newService.Create(ctx, namespace, ID, num, min, max)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		l := len(items)
		if l != 2 {
			t.Fatal("expected", 2, "got", l)
		}

		i1 := items[0]
		if i1 != 2 {
			t.Fatal("expected", 2, "got", i1)
		}
		i2 := items[1]
		if i2 != 3 {
			t.Fatal("expected", 3, "got", i2)
		}
	}

	// Fetch the next items.
	{
		items, err := newService.Create(ctx, namespace, ID, num, min, max)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		l := len(items)
		if l != 2 {
			t.Fatal("expected", 2, "got", l)
		}

		i1 := items[0]
		if i1 != 4 {
			t.Fatal("expected", 4, "got", i1)
		}
		i2 := items[1]
		if i2 != 5 {
			t.Fatal("expected", 5, "got", i2)
		}
	}

	// Delete the namespaced items for the test ID.
	{
		err := newService.Delete(ctx, namespace, ID)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}
	}

	// Fetch a new items. These should be based on the last items we created. So
	// the last items were 4 and 5, which implies the items we expect now are 6
	// and 7. We do not want to purge the last item pointer to rotate through the
	// available options all the time to be more efficient in certain edge cases.
	{
		items, err := newService.Create(ctx, namespace, ID, num, min, max)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		l := len(items)
		if l != 2 {
			t.Fatal("expected", 2, "got", l)
		}

		i1 := items[0]
		if i1 != 6 {
			t.Fatal("expected", 6, "got", i1)
		}
		i2 := items[1]
		if i2 != 7 {
			t.Fatal("expected", 7, "got", i2)
		}
	}
}

func Test_Service_Search(t *testing.T) {
	// Create a new storage and service.
	var err error
	var newService *Service
	var newStorage microstorage.Storage
	{
		newStorage, err = memory.New(memory.DefaultConfig())
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		config := DefaultConfig()
		config.Logger = microloggertest.New()
		config.Storage = newStorage
		newService, err = New(config)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}
	}

	// Prepare the test variables.
	ctx := context.TODO()
	namespace := "test-namespace"
	ID := "test-id"
	num := 2
	min := 2
	max := 9

	{
		_, err := newService.Create(ctx, namespace, ID, num, min, max)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		items, err := newService.Search(ctx, namespace, ID)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		l := len(items)
		if l != 2 {
			t.Fatal("expected", 2, "got", l)
		}

		i1 := items[0]
		if i1 != 2 {
			t.Fatal("expected", 2, "got", i1)
		}
		i2 := items[1]
		if i2 != 3 {
			t.Fatal("expected", 3, "got", i2)
		}
	}

	{
		_, err := newService.Create(ctx, namespace, ID, num, min, max)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		items, err := newService.Search(ctx, namespace, ID)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		l := len(items)
		if l != 4 {
			t.Fatal("expected", 4, "got", l)
		}

		i1 := items[0]
		if i1 != 2 {
			t.Fatal("expected", 2, "got", i1)
		}
		i2 := items[1]
		if i2 != 3 {
			t.Fatal("expected", 3, "got", i2)
		}
		i3 := items[2]
		if i3 != 4 {
			t.Fatal("expected", 4, "got", i3)
		}
		i4 := items[3]
		if i4 != 5 {
			t.Fatal("expected", 5, "got", i4)
		}
	}

	{
		err := newService.Delete(ctx, namespace, ID)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}
	}

	{
		_, err := newService.Search(ctx, namespace, ID)
		if !IsItemsNotFound(err) {
			t.Fatal("expected", true, "got", false)
		}
	}
}

func Test_Service_Create_NumTwo_DifferentIDs(t *testing.T) {
	// Create a new storage and service.
	var err error
	var newService *Service
	var newStorage microstorage.Storage
	{
		newStorage, err = memory.New(memory.DefaultConfig())
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}

		config := DefaultConfig()
		config.Logger = microloggertest.New()
		config.Storage = newStorage
		newService, err = New(config)
		if err != nil {
			t.Fatal("expected", nil, "got", err)
		}
	}

	testWithNameSpace := func(namespace string) {
		// Prepare the test variables.
		ctx := context.TODO()
		ID := "test-id"
		num := 2
		min := 2
		max := 9

		// Execute and assert the actually tested functionality. At first we fetch the
		// new items.
		{
			items, err := newService.Create(ctx, namespace, ID, num, min, max)
			if err != nil {
				t.Fatal("expected", nil, "got", err)
			}

			l := len(items)
			if l != 2 {
				t.Fatal("expected", 2, "got", l)
			}

			i1 := items[0]
			if i1 != 2 {
				t.Fatal("expected", 2, "got", i1)
			}
			i2 := items[1]
			if i2 != 3 {
				t.Fatal("expected", 3, "got", i2)
			}
		}

		// Fetch the next items.
		{
			items, err := newService.Create(ctx, namespace, ID, num, min, max)
			if err != nil {
				t.Fatal("expected", nil, "got", err)
			}

			l := len(items)
			if l != 2 {
				t.Fatal("expected", 2, "got", l)
			}

			i1 := items[0]
			if i1 != 4 {
				t.Fatal("expected", 4, "got", i1)
			}
			i2 := items[1]
			if i2 != 5 {
				t.Fatal("expected", 5, "got", i2)
			}
		}

		// Delete the namespaced items for the test ID.
		{
			err := newService.Delete(ctx, namespace, ID)
			if err != nil {
				t.Fatal("expected", nil, "got", err)
			}
		}

		// Fetch a new items. These should be based on the last items we created. So
		// the last items were 4 and 5, which implies the items we expect now are 6
		// and 7. We do not want to purge the last item pointer to rotate through the
		// available options all the time to be more efficient in certain edge cases.
		{
			items, err := newService.Create(ctx, namespace, ID, num, min, max)
			if err != nil {
				t.Fatal("expected", nil, "got", err)
			}

			l := len(items)
			if l != 2 {
				t.Fatal("expected", 2, "got", l)
			}

			i1 := items[0]
			if i1 != 6 {
				t.Fatal("expected", 6, "got", i1)
			}
			i2 := items[1]
			if i2 != 7 {
				t.Fatal("expected", 7, "got", i2)
			}
		}
	}

	testWithNameSpace("test-namespace-1")
	testWithNameSpace("test-namespace-2")
	testWithNameSpace("test-namespace-3")
}

func Test_nextItem(t *testing.T) {
	var used []int = []int{3, 4, 6}
	var min int = 2
	var max int = 9

	testCases := []struct {
		Latest       int
		Expected     int
		ErrorMatcher func(error) bool
	}{
		{
			Latest:       -2,
			Expected:     0,
			ErrorMatcher: IsExecutionFailed,
		},
		{
			Latest:       0,
			Expected:     0,
			ErrorMatcher: IsExecutionFailed,
		},
		{
			Latest:       1,
			Expected:     0,
			ErrorMatcher: IsExecutionFailed,
		},
		{
			Latest:       -1,
			Expected:     2,
			ErrorMatcher: nil,
		},
		{
			Latest:       2,
			Expected:     5,
			ErrorMatcher: nil,
		},
		{
			Latest:       3,
			Expected:     5,
			ErrorMatcher: nil,
		},
		{
			Latest:       4,
			Expected:     5,
			ErrorMatcher: nil,
		},
		{
			Latest:       5,
			Expected:     7,
			ErrorMatcher: nil,
		},
		{
			Latest:       6,
			Expected:     7,
			ErrorMatcher: nil,
		},
		{
			Latest:       7,
			Expected:     8,
			ErrorMatcher: nil,
		},
		{
			Latest:       8,
			Expected:     9,
			ErrorMatcher: nil,
		},
		{
			Latest:       9,
			Expected:     2,
			ErrorMatcher: nil,
		},
		{
			Latest:       10,
			Expected:     0,
			ErrorMatcher: IsExecutionFailed,
		},
	}

	for i, tc := range testCases {
		newVNI, err := nextItem(used, min, max, tc.Latest)

		if err != nil && tc.ErrorMatcher == nil {
			t.Fatal("case", i+1, "expected", nil, "got", err)
		}
		if tc.ErrorMatcher != nil && !tc.ErrorMatcher(err) {
			t.Fatal("case", i+1, "expected", true, "got", false)
		}
		if tc.Expected != newVNI {
			t.Fatal("case", i+1, "expected", tc.Expected, "got", newVNI)
		}
	}
}
