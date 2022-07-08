/*
Copyright 2022 The SODA Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package options

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
)

const (
	unixSocketPath = "/tmp/samplevolumeprovider.sock"
)

type ServiceFlags struct {
	UnixSocketPath string `json:"unixSocketPath"`
}

func NewServiceFlags() *ServiceFlags {
	return &ServiceFlags{
		UnixSocketPath: unixSocketPath,
	}
}

// AddFlags exposes available command line options
func (options *ServiceFlags) AddFlags(fs *pflag.FlagSet) {
	fs.StringVarP(&options.UnixSocketPath, "socket", "s",
		options.UnixSocketPath, "Unix socket path")
}

// Apply checks validity of available command line options
func (options *ServiceFlags) Apply() error {
	// no validation currently
	return nil
}

func (options *ServiceFlags) Print() {
	printPretty, err := json.MarshalIndent(options, "", "    ")
	if err != nil {
		log.Infof("Service config %+v", *options)
	}
	log.Infof("Service config \n%s", string(printPretty))
}
