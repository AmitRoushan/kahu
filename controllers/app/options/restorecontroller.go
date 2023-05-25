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
	"github.com/spf13/pflag"

	"github.com/soda-cdm/kahu/controllers/restore"
)

type RestoreControllerFlags struct {
	*restore.Config
}

func NewRestoreControllerFlags() *RestoreControllerFlags {
	return &RestoreControllerFlags{
		&restore.Config{
			ConcurrentRestoreCount: restore.DefaultConcurrentRestoreCount,
		},
	}
}

// AddFlags exposes available command line options
func (opt *RestoreControllerFlags) AddFlags(fs *pflag.FlagSet) {
	fs.IntVar(&opt.ConcurrentRestoreCount, "concurrentRestoreCount", opt.ConcurrentRestoreCount,
		"max number of concurrent restore")
}

// ApplyTo checks validity of available command line options
func (opt *RestoreControllerFlags) ApplyTo(cfg *restore.Config) error {
	cfg.ConcurrentRestoreCount = opt.ConcurrentRestoreCount
	return nil
}
