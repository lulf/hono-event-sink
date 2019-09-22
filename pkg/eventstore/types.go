/*
 * Copyright 2019, Ulf Lilleengen
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package eventstore

type Event struct {
	CreationTime int64  `json:"creationTime"`
	DeviceId     string `json:"deviceId"`
	Payload      string `json:"payload"`
}
