package gocouchstore

/*
#cgo LDFLAGS: -lcouchstore

#include <stdlib.h>
#include <string.h>
#include "libcouchstore/couch_db.h"
#include "libcouchstore/couch_common.h"


void create_doc(Doc *doc, char *key, char *data) {

	memset(doc, 0, sizeof(Doc));

	doc->id.buf = (char *)malloc(strlen(key)+1);
	doc->id.size = strlen(key)+1;
	memcpy(doc->id.buf, key, doc->id.size);

	doc->data.buf = (char *)malloc(strlen(data)+1);
	doc->data.size = strlen(data)+1;
	memcpy(doc->data.buf, data, doc->data.size);
}

void free_doc(Doc *doc) {

	free(doc->id.buf);
	free(doc->data.buf);
}

void create_info(DocInfo *info,Doc doc,char* meta) {

	memset(info, 0, sizeof(DocInfo));

	info->id = doc.id;
	info->rev_seq = 1;
	info->rev_meta.size = strlen(meta) + 1;
	info->rev_meta.buf = (char *)malloc(strlen(meta)+1);
	memcpy(info->rev_meta.buf, meta, info->rev_meta.size);
	info->deleted = 0;
	info->content_meta = 0;
}


void free_info(DocInfo *info) {

	free(info->rev_meta.buf);
}

*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
)

type Errno int

func (e Errno) Error() string {
	s := errText[e]
	if s == "" {
		return fmt.Sprintf("errno %d", int(e))
	}
	return s
}

var errText = map[Errno]string{
	-1:  "COUCHSTORE_ERROR_OPEN_FILE",
	-2:  "COUCHSTORE_ERROR_CORRUPT",
	-3:  "COUCHSTORE_ERROR_ALLOC_FAIL",
	-4:  "COUCHSTORE_ERROR_READ",
	-5:  "COUCHSTORE_ERROR_DOC_NOT_FOUND",
	-6:  "COUCHSTORE_ERROR_NO_HEADER",
	-7:  "COUCHSTORE_ERROR_WRITE",
	-8:  "COUCHSTORE_ERROR_HEADER_VERSION",
	-9:  "COUCHSTORE_ERROR_CHECKSUM_FAIL",
	-10: "COUCHSTORE_ERROR_INVALID_ARGUMENTS",
	-11: "COUCHSTORE_ERROR_NO_SUCH_FILE",
	-12: "COUCHSTORE_ERROR_CANCEL",
	-13: "COUCHSTORE_ERROR_REDUCTION_TOO_LARGE",
	-14: "COUCHSTORE_ERROR_REDUCER_FAILURE",
	-15: "COUCHSTORE_ERROR_FILE_CLOSED",
	-16: "COUCHSTORE_ERROR_DB_NO_LONGER_VALID",
}

type Conn struct {
	db *C.Db
}

func OpenRW(filename string) (*Conn, error) {

	var db *C.Db

	dbname := C.CString(filename)
	defer C.free(unsafe.Pointer(dbname))

	rv := C.couchstore_open_db(dbname, C.COUCHSTORE_OPEN_FLAG_CREATE, &db)

	if rv != 0 {
		return nil, errors.New(Errno(rv).Error())
	}

	if db == nil {
		return nil, errors.New("couchstore succeeded without returning a database")
	}

	return &Conn{db}, nil
}

func OpenRO(filename string) (*Conn, error) {

	var db *C.Db

	dbname := C.CString(filename)
	defer C.free(unsafe.Pointer(dbname))

	rv := C.couchstore_open_db(dbname, C.COUCHSTORE_OPEN_FLAG_RDONLY, &db)

	if rv != 0 {
		return nil, errors.New(Errno(rv).Error())
	}

	if db == nil {
		return nil, errors.New("couchstore succeeded without returning a database")
	}

	return &Conn{db}, nil
}

func (c *Conn) Put(key, meta, value []byte) error {

	k := C.CString(string(key))
	defer C.free(unsafe.Pointer(k))

	m := C.CString(string(meta))
	defer C.free(unsafe.Pointer(m))

	v := C.CString(string(value))
	defer C.free(unsafe.Pointer(v))

	var doc C.Doc
	var info C.DocInfo

	C.create_doc(&doc, k, v)
	C.create_info(&info, doc, m)

	rv := C.couchstore_save_document(c.db, &doc, &info, 0)
	if rv != 0 {
		return errors.New(Errno(rv).Error())
	}

	C.free_doc(&doc)
	C.free_info(&info)
	return nil
}

func (c *Conn) Get(key []byte) ([]byte, error) {

	var doc C.Doc
	var docOut *C.Doc

	k := C.CString(string(key))
	defer C.free(unsafe.Pointer(k))

	v := C.CString(string(""))
	defer C.free(unsafe.Pointer(v))

	C.create_doc(&doc, k, v)

	rv := C.couchstore_open_document(c.db, unsafe.Pointer(doc.id.buf), doc.id.size, &docOut, 0)

	if rv != 0 {
		return nil, errors.New(Errno(rv).Error())
	}

	value := docOut.data.buf
	vallen := docOut.data.size

	defer C.couchstore_free_document(docOut)
	return C.GoBytes(unsafe.Pointer(value), C.int(vallen)), nil

}

func (c *Conn) Delete(key []byte) error {

	k := C.CString(string(key))
	defer C.free(unsafe.Pointer(k))

	m := C.CString(string(""))
	defer C.free(unsafe.Pointer(m))

	var doc C.Doc
	var info C.DocInfo

	C.create_doc(&doc, k, m)
	C.create_info(&info, doc, m)

	rv := C.couchstore_save_document(c.db, nil, &info, 0)
	if rv != 0 {
		return errors.New(Errno(rv).Error())
	}

	C.free_doc(&doc)
	C.free_info(&info)
	return nil
}

func (c *Conn) Compact(newfilename string) error {

	f := C.CString(newfilename)
	defer C.free(unsafe.Pointer(f))

	rv := C.couchstore_compact_db(c.db, f)
	if rv != 0 {
		return errors.New(Errno(rv).Error())
	}
	return nil
}

func (c *Conn) Commit() error {
	rv := C.couchstore_commit(c.db)
	if rv != 0 {
		return errors.New(Errno(rv).Error())
	}
	return nil
}

func (c *Conn) Close() error {
	rv := C.couchstore_close_db(c.db)
	if rv != 0 {
		return errors.New(Errno(rv).Error())
	}
	return nil
}
