package processor

import (
	"context"
	"os"
	"strings"
)

// CredentialProvider provides the Access API token that every call of the Processor Runtime API
// presents. It is asked before every call, so a new token applies from the next call. When it
// fails, Run ends with ErrorUnauthenticated.
type CredentialProvider interface {
	Credential(ctx context.Context) (string, error)
}

// CredentialProviderFunc is a CredentialProvider function.
type CredentialProviderFunc func(ctx context.Context) (string, error)

func (f CredentialProviderFunc) Credential(ctx context.Context) (string, error) { return f(ctx) }

// StaticToken provides token. Formatting it never shows the token.
func StaticToken(token string) CredentialProvider { return staticToken(token) }

type staticToken string

func (t staticToken) Credential(context.Context) (string, error) { return string(t), nil }

func (staticToken) String() string { return "StaticToken([REDACTED])" }

func (staticToken) GoString() string { return "StaticToken([REDACTED])" }

// FileToken provides the token that a file holds, without surrounding white space. It reads the
// file for every call, so that replacing the file rotates the token.
func FileToken(path string) CredentialProvider { return fileToken(path) }

type fileToken string

func (path fileToken) Credential(context.Context) (string, error) {
	content, err := os.ReadFile(string(path))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(content)), nil
}

func (path fileToken) String() string { return "FileToken(" + string(path) + ")" }

// bearer presents the token of a CredentialProvider as the authorization metadata of every call.
type bearer struct {
	provider CredentialProvider
	// secure is whether the connection uses TLS.
	secure bool
}

func (b bearer) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	token, err := b.provider.Credential(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]string{"authorization": "Bearer " + token}, nil
}

func (b bearer) RequireTransportSecurity() bool { return b.secure }
