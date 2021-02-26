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
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// We cannot guarantee that a particular environment variable doesn't exist,
// so we generate a sufficiently long random string name.
const undefEnvNameLen = 20

func randomEnvString(t *testing.T, n int) string {
	t.Helper()
	chars := []byte("0123456789ABCDEFGHIJKLMNOPQRSWTUVXYZabcdefghijklmnopqrstuvwyxz_")
	var result strings.Builder
	result.Grow(n)
	for i := 0; i < n; i++ {
		result.WriteByte(chars[rand.Intn(len(chars))])
	}
	return result.String()
}

// setTestEnv takes a environment value and stores it into a random environment variable.
// Returns the variable name.
func setTestEnv(t *testing.T, value string) string {
	t.Helper()
	key := "open_saves_test_" + t.Name() + randomEnvString(t, 10)
	if err := os.Setenv(key, value); err != nil {
		t.Fatalf("Setenv failed: %v", err)
	}
	t.Cleanup(func() {
		os.Unsetenv(key)
	})
	return key
}

func TestEnv_GetEnvVarUInt(t *testing.T) {
	const defValue = uint64(12345)
	const testValue = uint64(42)

	assert.Equal(t, defValue, GetEnvVarUInt(randomEnvString(t, undefEnvNameLen), defValue))

	keyEmpty := setTestEnv(t, "")
	assert.Equal(t, defValue, GetEnvVarUInt(keyEmpty, defValue))

	key := setTestEnv(t, "42")
	assert.Equal(t, testValue, GetEnvVarUInt(key, 0))

	keyMalformatted := setTestEnv(t, "not a number")
	assert.Equal(t, defValue, GetEnvVarUInt(keyMalformatted, defValue))
}

func TestEnv_GetEnvVarString(t *testing.T) {
	const defValue = "Hello, World"
	const testStr = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do "

	assert.Equal(t, defValue, GetEnvVarString(randomEnvString(t, undefEnvNameLen), defValue))

	keyEmpty := setTestEnv(t, "")
	assert.Equal(t, defValue, GetEnvVarString(keyEmpty, defValue))

	key := setTestEnv(t, testStr)
	assert.Equal(t, testStr, GetEnvVarString(key, defValue))
}

func TestEnv_GetEnvVarDuration(t *testing.T) {
	const defValue = 42 * time.Minute
	const testValue = 123 * time.Hour

	assert.Equal(t, defValue, GetEnvVarDuration(randomEnvString(t, undefEnvNameLen), defValue))

	keyEmpty := setTestEnv(t, "")
	assert.Equal(t, defValue, GetEnvVarDuration(keyEmpty, defValue))

	key := setTestEnv(t, "123h")
	assert.Equal(t, testValue, GetEnvVarDuration(key, defValue))

	keyMalformatted := setTestEnv(t, "not a duration")
	assert.Equal(t, defValue, GetEnvVarDuration(keyMalformatted, defValue))
}
