package httpserver

import (
	"fmt"
	"strings"
)

// Path contains the following path structure:
// /{prefix}/{tenantID}/{suffix}
type Path struct {
	Prefix    string
	AuthToken string
	Suffix    string
}

// ParsePath parses the given path.
func ParsePath(path string) (*Path, error) {
	// The path must have the following form:
	// /{prefix}/{tenantID}/{suffix}
	//
	// - prefix must contain `select`, `insert` or `delete`.
	// - tenantID contains `accountID[:projectID]`, where projectID is optional.
	//   tenantID may also contain `multitenant` string. In this case the accountID and projectID
	//   are obtained from vm_account_id and vm_project_id labels of the ingested samples.
	// - suffix contains arbitrary suffix.
	//
	// prefix must be used for the routing to the appropriate service
	// in the cluster - either vminsert or vmselect.
	s := skipPrefixSlashes(path)
	n := strings.IndexByte(s, '/')
	if n < 0 {
		return nil, fmt.Errorf("cannot find {prefix} in %q; expecting /{prefix}/{tenantID}/{suffix} format; "+
			"see https://docs.victoriametrics.com/victoriametrics/cluster-victoriametrics/#url-format", path)
	}
	prefix := s[:n]

	s = skipPrefixSlashes(s[n+1:])
	n = strings.IndexByte(s, '/')
	if n < 0 {
		return nil, fmt.Errorf("cannot find {tenantID} in %q; expecting /{prefix}/{tenantID}/{suffix} format; "+
			"see https://docs.victoriametrics.com/victoriametrics/cluster-victoriametrics/#url-format", path)
	}
	tenantID := s[:n]

	s = skipPrefixSlashes(s[n+1:])

	// Substitute double slashes with single slashes in the path, since such slashes
	// may appear due improper copy-pasting of the url.
	suffix := strings.Replace(s, "//", "/", -1)

	p := &Path{
		Prefix:    prefix,
		AuthToken: tenantID,
		Suffix:    suffix,
	}
	return p, nil
}

// skipPrefixSlashes remove double slashes which may appear due
// improper copy-pasting of the url
func skipPrefixSlashes(s string) string {
	for len(s) > 0 && s[0] == '/' {
		s = s[1:]
	}
	return s
}
