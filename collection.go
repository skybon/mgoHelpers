package mgoHelpers

import (
	"sync"

	"gopkg.in/mgo.v2/bson"
)

type MongoEntry interface {
	BsonID() bson.ObjectId
	SetBsonID(bson.ObjectId)
}

type EntryErrorPair struct {
	Entry MongoEntry
	Error error
}

type EntryErrorPairs []EntryErrorPair

func (p *EntryErrorPairs) CheckPartFail() bool {
	for _, v := range *p {
		if v.Error != nil {
			return true
		}
	}
	return false
}

func (p *EntryErrorPairs) MakeEntrySlice() []MongoEntry {
	entries := make([]MongoEntry, len(*p))
	for i, v := range *p {
		entries[i] = v.Entry
	}

	return entries
}

// MongoCollection represents abstact collection in MongoDB. It supports CRUD but should not be used directly. Instead you have to write a wrapper for your own data type.
type MongoCollection struct {
	sync.Mutex
	MongoStorageInfo
	factoryFunc func(*MongoCollection, interface{}) MongoEntry
}

func (c *MongoCollection) MutexExec(cb func()) {
	c.Lock()
	defer c.Unlock()
	cb()
}

// SetFactoryFunc sets the object factory function used by Create for creation of new objects.
func (c *MongoCollection) SetFactoryFunc(factoryFunc func(*MongoCollection, interface{}) MongoEntry) {
	c.factoryFunc = factoryFunc
}

func (c *MongoCollection) insertCore(entries []MongoEntry) error {
	var inEntries = make([]interface{}, len(entries))
	for i, v := range entries {
		inEntries[i] = v
	}

	return c.Database.Insert(c.Collection, inEntries...)
}

func (c *MongoCollection) makeOne(factoryFuncParam interface{}) (entry MongoEntry, err error) {
	if c.factoryFunc == nil {
		return entry, errNoFactoryFunc
	}
	entry = c.factoryFunc(c, factoryFuncParam)

	return entry, nil
}

func (c *MongoCollection) createCore(factoryFuncParamSet []interface{}) (entries []MongoEntry, err error) {
	var entryerr EntryErrorPairs
	for _, params := range factoryFuncParamSet {
		entry, eErr := c.makeOne(params)
		entryerr = append(entryerr, EntryErrorPair{entry, eErr})
	}

	if entryerr.CheckPartFail() {
		return nil, errBulkOpAborted
	}

	entries = entryerr.MakeEntrySlice()
	err = c.insertCore(entries)

	return entries, err
}

// Insert adds a ready-made entry into the database.
func (c *MongoCollection) Insert(entry MongoEntry) (err error) {
	c.MutexExec(func() { err = c.insertCore([]MongoEntry{entry}) })

	return err
}

func (c *MongoCollection) InsertBulk(entries []MongoEntry) (err error) {
	c.MutexExec(func() { err = c.insertCore(entries) })

	return err
}

// Create creates a new entry from specified param and factory function and inserts it into database. Please note that unless your factory depends on items in the collection you should run Insert instead.
func (c *MongoCollection) Create(factoryFuncParams interface{}) (entry MongoEntry, err error) {
	var entries []MongoEntry
	c.MutexExec(func() { entries, err = c.createCore([]interface{}{factoryFuncParams}) })

	if err != nil {
		return nil, err
	}

	return entries[0], nil
}

func (c *MongoCollection) CreateBulk(factoryFuncParamSet []interface{}) (entries []MongoEntry, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errPanic
		}
	}()

	c.MutexExec(func() { entries, err = c.createCore(factoryFuncParamSet) })
	if err != nil {
		return nil, err
	}

	return entries, nil
}

// Read returns the entry that matches specified entry ID.
func (c *MongoCollection) Read(entryID string, result MongoEntry) bool {
	return c.Database.FindById(c.Collection, entryID, result)
}

// ReadAll returns all entries in the database. The first argument *must* be a pointer to a slice.
func (c *MongoCollection) ReadAll(result interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errPanic
		}
	}()
	return c.Database.FindAll(c.Collection, result)
}

// Update modifies entry based on input parameters.
func (c *MongoCollection) Update(entryID string, factoryFuncParam interface{}) (entry MongoEntry, status error) {
	c.MutexExec(func() {
		b, idParseErr := GetObjectIDFromString(entryID)

		var err error
		var newEntry MongoEntry

		if idParseErr != nil {
			err = idParseErr
		} else {
			newEntry = c.factoryFunc(c, factoryFuncParam)
			newEntry.SetBsonID(b)
			err = c.Database.Update(c.Collection, b, newEntry)
		}
		entry = newEntry
		status = err
	})
	return entry, status
}

// Delete removes entry from the database.
func (c *MongoCollection) Delete(entryID string) (status error) {
	c.MutexExec(func() {
		var err error
		b, idParseErr := GetObjectIDFromString(entryID)

		if idParseErr != nil {
			err = idParseErr
		} else {
			err = c.Database.Remove(c.Collection, b)
		}
		status = err
	})

	return status
}

// DeleteAll removes all entries from the collection.
func (c *MongoCollection) DeleteAll() (status error) {
	c.MutexExec(func() { status = c.Database.RemoveAll(c.Collection) })

	return status
}

// NewMongoCollection creates a new instance of the MongoDB collection.
func NewMongoCollection(dbInstance *MongoDb, collName string) *MongoCollection {
	c := MongoCollection{}
	c.Database = dbInstance
	c.Collection = collName

	return &c
}
