set -o errexit
set -o nounset
set -o pipefail

# Ensure the go and minimum version.
kahu::golang::verify_go_version() {
  if [[ -z "$(command -v go)" ]]; then
    echo "
Can't find 'go' in PATH, please fix and retry.
See http://golang.org/doc/install for installation instructions.
"
    return 2
  fi

  local go_version
  IFS=" " read -ra go_version <<< "$(GOFLAGS='' go version)"
  local minimum_go_version
  minimum_go_version=go1.16.0
  if [[ "${minimum_go_version}" != $(echo -e "${minimum_go_version}\n${go_version[2]}" | sort -s -t. -k 1,1 -k 2,2n -k 3,3n | head -n1) && "${go_version[2]}" != "devel" ]]; then
    echo"
Detected go version: ${go_version[*]}.
Kubernetes requires ${minimum_go_version} or greater.
Please install ${minimum_go_version} or later.
"
    return 2
  fi
}

kahu::golang::verify_go_version
