// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"github.com/googleforgames/open-saves/internal/app/server"
	"github.com/googleforgames/open-saves/internal/pkg/cmd"
	"github.com/googleforgames/open-saves/internal/pkg/config"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func main() {
	configPath := cmd.GetEnvVarString("OPEN_SAVES_CONFIG", "/configs/")
	cfg, err := config.Load(configPath)
	if err != nil {
		panic(fmt.Errorf("error loading config file: %w", err))
	}

	ll, err := log.ParseLevel(viper.GetString(config.LogLevel))
	if err != nil {
		ll = log.InfoLevel
	}
	log.SetLevel(ll)
	log.Infof("Log level is: %s", ll.String())

	ctx := context.Background()
	if err := server.Run(ctx, "tcp", cfg); err != nil {
		log.Fatalf("got error starting server: %v", err)
	}
}
