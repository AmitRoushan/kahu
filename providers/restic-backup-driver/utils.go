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

// Package server implements Restic provider service interfaces
package restic

import (
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
)

const (
	DEFAULT_PASSWORD_FILE_PATH = "/tmp"
	PASSWORD_FILE              = "passwd"
	PASSWD                     = "a2FodS1wYXNzd2QK"
	PV_NAME                    = "PV"
)

// passwdPath returns a path on disk where the password key defined by
// the given selector is serialized.
func passwdPath() (string, error) {
	directory, err := filepath.Abs(filepath.Dir(os.Args[0])) //get the current working directory
	if err != nil {
		directory = DEFAULT_PASSWORD_FILE_PATH
	}

	keyFilePath := filepath.Join(directory, PASSWORD_FILE)

	file, err := os.OpenFile(keyFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return "", errors.Wrap(err, "unable to open credentials file for writing")
	}

	if _, err := file.Write([]byte(PASSWD)); err != nil {
		return "", errors.Wrap(err, "unable to write credentials to store")
	}

	if err := file.Close(); err != nil {
		return "", errors.Wrap(err, "unable to close credentials file")
	}

	return keyFilePath, nil
}

func tags(pv *corev1.PersistentVolume) map[string]string {
	tags := make(map[string]string)
	tags[PV_NAME] = pv.Name
	return tags
}

func remoteRepoIdentifier(location, pvName string) (string, error) {
	locationInfo, err := os.Stat(location)
	if err != nil {
		return "", errors.Errorf("failed to check stat of repository location path. %s", err)
	}

	if locationInfo.IsDir() {
		remoteRepo := filepath.Join(location, pvName)
		if err := os.MkdirAll(remoteRepo, os.ModePerm); err != nil {
			return remoteRepo, errors.Errorf("failed to create remote repo location path. %s", err)
		}
		return remoteRepo, nil
	}

	// if file, it can be secret file for S3 or other repository info
	// currently not handled

	return "", errors.Errorf("not supported")
}
