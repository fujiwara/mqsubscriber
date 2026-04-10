//go:build windows

package subscriber

import "os/exec"

func setupProcessGroup(cmd *exec.Cmd) {
	cmd.Cancel = func() error {
		return cmd.Process.Kill()
	}
}
