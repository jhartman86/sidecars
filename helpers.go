package sidecars

import (
	"pkg/sys/errz"
	"pkg/sys/typed"
	"strings"
)

func errToStackText(err error) typed.String {
	return typed.NewStringApply(errz.ErrorStack(err).String(), trimPreAndSuffixNewLines)
}

func trimPreAndSuffixNewLines(s string) string {
	return strings.TrimPrefix(strings.TrimSuffix(s, "\n"), "\n")
}
