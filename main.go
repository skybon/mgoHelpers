package mgoHelpers

import (
	"errors"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	mongoConnectionTimeout = 5 * time.Second
)

var errNoFactoryFunc = errors.New("No factory function")
var errBulkOpAborted = errors.New("Bulk operation aborted")
var errPanic = errors.New("Procedure panic")

type MongoDb struct {
	sess *mgo.Session
}

func (db *MongoDb) Connect(dsn string) error {
	var err error

	db.sess, err = mgo.DialWithTimeout(dsn, mongoConnectionTimeout)

	return err
}
func (db *MongoDb) Disconnect() {
	db.sess.Close()
}

func (db *MongoDb) Insert(coll string, v ...interface{}) error {
	sess := db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Insert(v...)
}

func (db *MongoDb) Find(coll string, query map[string]interface{}, v interface{}) error {
	sess := db.sess.Copy()
	defer sess.Close()

	bsonQuery := bson.M{}

	for k, qv := range query {
		bsonQuery[k] = qv
	}

	return sess.DB("").C(coll).Find(bsonQuery).All(v)
}

func (db *MongoDb) FindById(coll string, id string, v interface{}) bool {
	sess := db.sess.Copy()
	defer sess.Close()

	return mgo.ErrNotFound != sess.DB("").C(coll).FindId(id).One(v)
}

func (db *MongoDb) FindAll(coll string, v interface{}) error {
	sess := db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Find(bson.M{}).All(v)
}

func (db *MongoDb) Update(coll string, id interface{}, v interface{}) error {
	sess := db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).Update(bson.M{"_id": id}, bson.M{"$set": v})
}

func (db *MongoDb) Remove(coll string, id interface{}) error {
	sess := db.sess.Copy()
	defer sess.Close()

	_, err := sess.DB("").C(coll).RemoveAll(bson.M{"_id": id})

	return err
}

func (db *MongoDb) RemoveAll(coll string) error {
	sess := db.sess.Copy()
	defer sess.Close()

	_, err := sess.DB("").C(coll).RemoveAll(bson.M{})

	return err
}

func (db *MongoDb) SessExec(cb func(*mgo.Session)) {
	var sess = db.sess.Copy()
	defer sess.Close()

	cb(sess)
}

func GetDb() *MongoDb { return &MongoDb{} }

type DbEntryBase struct {
	ID bson.ObjectId `bson:"_id,omitempty" json:"objectid"`
}

func (e *DbEntryBase) BsonID() bson.ObjectId { return e.ID }

func (e *DbEntryBase) SetBsonID(id bson.ObjectId) { e.ID = id }

type MongoStorageInfo struct {
	Database   *MongoDb
	Collection string
}

func GetObjectIDFromString(s string) (bson.ObjectId, error) {
	b := bson.NewObjectId()
	err := b.UnmarshalJSON([]byte(`"` + s + `"`))
	return b, err
}
