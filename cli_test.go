package subscriber

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadEnvFile(t *testing.T) {
	dir := t.TempDir()
	envFile := filepath.Join(dir, ".env")
	content := `FOO=bar
BAZ=qux
# comment line
EMPTY=
QUOTED="hello world"
`
	if err := os.WriteFile(envFile, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	// Clean up env vars after test
	for _, key := range []string{"FOO", "BAZ", "EMPTY", "QUOTED"} {
		t.Setenv(key, "")
		os.Unsetenv(key)
	}

	if err := loadEnvFile(envFile); err != nil {
		t.Fatalf("loadEnvFile failed: %v", err)
	}

	tests := []struct {
		key  string
		want string
	}{
		{"FOO", "bar"},
		{"BAZ", "qux"},
		{"EMPTY", ""},
		{"QUOTED", "hello world"},
	}
	for _, tt := range tests {
		if got := os.Getenv(tt.key); got != tt.want {
			t.Errorf("env %s = %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestLoadEnvFileNotFound(t *testing.T) {
	err := loadEnvFile("/nonexistent/.env")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestLoadEnvFileInvalid(t *testing.T) {
	dir := t.TempDir()
	envFile := filepath.Join(dir, ".env")
	// invalid: missing value separator
	content := `INVALID LINE WITHOUT EQUALS`
	if err := os.WriteFile(envFile, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	err := loadEnvFile(envFile)
	if err == nil {
		t.Fatal("expected error for invalid env file")
	}
}
