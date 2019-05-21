/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package datastore

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestInitialize(t *testing.T) {
	os.Remove("test.db")
	ds, err := NewSqliteDatastore("test.db")
	assert.Nil(t, err)
	assert.NotNil(t, ds)
}
