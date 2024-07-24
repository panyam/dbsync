package dbsync

type DBItem struct {
	Key  any
	Data any
}

type Selection interface {
	// ID of the this selection
	Id() string

	Execute() error

	// Low watermark id of this selection
	LowWatermark() string

	// High watermark id of this selection
	HighWatermark() string

	// Gets all items currently remaining in the selection
	// TODO - Should we allow iteration on this in case we
	// are ok to have a *really* large dataset as part of this
	Items() map[any]DBItem

	// Get the value of an item in this selection given its key.
	GetItem(key any) (value any, exists bool)

	// Removes an item from this collection
	RemoveItem(key any) any

	// Clears all items from this selection to release any storage needed
	Clear() bool
}
