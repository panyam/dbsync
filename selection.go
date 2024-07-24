package dbsync

import (
	gut "github.com/panyam/goutils/utils"
)

type DBItem struct {
	Key  any
	Data any
}

type InMemSelection struct {
	// ID of the selection
	id string

	// Entries as a result of the selection
	items map[any]DBItem
}

// Creates a new in memory selection with a random ID
func NewInMemSelection() *InMemSelection {
	return &InMemSelection{
		id: gut.RandString(30, ""),
	}
}

// Returns the Unique ID of this selection
func (i *InMemSelection) ID() string {
	return i.id
}

// Gets all items currently remaining in the selection
// TODO - Should we allow iteration on this in case we
// are ok to have a *really* large dataset as part of this
func (i *InMemSelection) Items() map[any]DBItem {
	return i.items
}

// Get the value of an item in this selection given its key.
func (i *InMemSelection) GetItem(key any) (value any, exists bool) {
	if i.items == nil {
		return nil, false
	}
	value, exists = i.items[key]
	return
}

// Removes an item from this collection
func (i *InMemSelection) RemoveItem(key any) (out any) {
	if i.items != nil {
		var ok bool
		if out, ok = i.items[key]; ok {
			delete(i.items, key)
		}
	}
	return
}

// Clears all items from this selection to release any storage needed
func (i *InMemSelection) Clear() bool {
	i.items = nil
	return true
}
