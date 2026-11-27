package redact

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestStringsRedactsKnownSecretPatterns(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		mustHide string
		mustKeep string
	}{
		{
			name:     "openai_key",
			input:    "OPENAI_API_KEY=sk-1234567890abcdefghijklmnop",
			mustHide: "sk-1234567890abcdefghijklmnop",
			mustKeep: "[REDACTED_SECRET]",
		},
		{
			name:     "aws_access_key",
			input:    "aws key AKIAIOSFODNN7EXAMPLE leaked",
			mustHide: "AKIAIOSFODNN7EXAMPLE",
			mustKeep: "[REDACTED_SECRET]",
		},
		{
			name:     "bearer_token",
			input:    "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.abcdef123456",
			mustHide: "eyJhbGciOiJIUzI1NiJ9.abcdef123456",
			mustKeep: "Bearer [REDACTED_SECRET]",
		},
		{
			name:     "url_password",
			input:    "postgres://admin:supersecretpass@db.local:5432/app",
			mustHide: "supersecretpass",
			mustKeep: "postgres://admin:[REDACTED_SECRET]@db.local:5432/app",
		},
		{
			name:     "cookie_header",
			input:    "Cookie: session=xyz123secret\nnext=ok",
			mustHide: "xyz123secret",
			mustKeep: "Cookie: [REDACTED_SECRET]\nnext=ok",
		},
		{
			name:     "password_assignment",
			input:    `password="my_secret_value_123"`,
			mustHide: "my_secret_value_123",
			mustKeep: "[REDACTED_SECRET]",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Strings(tc.input)
			if strings.Contains(got, tc.mustHide) {
				t.Fatalf("secret %q leaked in %q", tc.mustHide, got)
			}
			if !strings.Contains(got, tc.mustKeep) {
				t.Fatalf("expected %q in %q", tc.mustKeep, got)
			}
		})
	}
}

func TestJSONRedactsRecursively(t *testing.T) {
	raw := json.RawMessage(`{"cmd":"curl -H 'Authorization: Bearer abcdefghijklmnop1234' https://u:pass123@host/x","nested":["sk-abcdefghijklmnopqrstuvwxyz1234"]}`)
	got := JSON(raw)
	if !json.Valid(got) {
		t.Fatalf("redacted JSON is invalid: %s", got)
	}
	s := string(got)
	if strings.Contains(s, "abcdefghijklmnop1234") || strings.Contains(s, "pass123") || strings.Contains(s, "sk-abcdefghijklmnopqrstuvwxyz1234") {
		t.Fatalf("secret leaked in JSON: %s", s)
	}
}
