// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"os"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

// GetEnvVarUInt returns an decimal uint64 value of an environmental variable specified by name.
// Returns defValue when the variable is empty (e.g. not defined), or in case of parsing error.
func GetEnvVarUInt(name string, defValue uint64) uint64 {
	if value := os.Getenv(name); value != "" {
		uval, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			log.Warningf("failed to parse %s env variable, default to %v, err = %v", name, defValue, err)
			uval = defValue
		}
		return uval
	}
	return defValue
}

// GetEnvVarString returns a string value of an environmental variable speficied by name.
// Returns defValue if the variable is empty (e.g. not defined).
func GetEnvVarString(name string, defValue string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return defValue
}

// GetEnvVarDuration returns a time.Duration value of an environmental variable specified by name.
// Returns defValue if the variable is not defined, or in case of parsing error.
func GetEnvVarDuration(name string, defValue time.Duration) time.Duration {
	if value := os.Getenv(name); value != "" {
		duration, err := time.ParseDuration(value)
		if err != nil {
			log.Warningf("failed to parse %s env variable, default to %v, err = %v", name, defValue, err)
			duration = defValue
		}
		return duration
	}
	return defValue
}
