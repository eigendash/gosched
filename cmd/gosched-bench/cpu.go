package main

import (
	"fmt"
	"os/exec"
	"runtime"
	"strings"
)

// coreTopology describes heterogeneous core counts when the platform exposes
// them; a speedup curve on a 8P+4E machine is not the same experiment as one on
// twelve equal cores, so the results files say which it was.
func coreTopology() string {
	if runtime.GOOS != "darwin" {
		return "unknown"
	}
	get := func(key string) string {
		out, err := exec.Command("sysctl", "-n", key).Output()
		if err != nil {
			return ""
		}
		return strings.TrimSpace(string(out))
	}
	perf := get("hw.perflevel0.physicalcpu")
	eff := get("hw.perflevel1.physicalcpu")
	if perf == "" || eff == "" {
		return "unknown"
	}
	return fmt.Sprintf("%s performance + %s efficiency cores", perf, eff)
}

// osVersion reports the OS build, because "darwin/arm64" is not enough to
// reproduce a measurement on a machine whose scheduler behaviour changed
// between releases.
func osVersion() string {
	switch runtime.GOOS {
	case "darwin":
		out, err := exec.Command("sw_vers", "-productVersion").Output()
		if err == nil {
			if s := strings.TrimSpace(string(out)); s != "" {
				return "macOS " + s
			}
		}
	case "linux":
		if out, err := exec.Command("sh", "-c", "uname -r").Output(); err == nil {
			if s := strings.TrimSpace(string(out)); s != "" {
				return "Linux " + s
			}
		}
	}
	return runtime.GOOS
}

// cpuModel reports the host CPU for the results files. It shells out only to
// read-only system utilities, and falls back to "unknown" so the harness still
// runs on a platform it does not know.
func cpuModel() string {
	switch runtime.GOOS {
	case "darwin":
		if out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
			if s := strings.TrimSpace(string(out)); s != "" {
				return s
			}
		}
	case "linux":
		if out, err := exec.Command("sh", "-c", "grep -m1 'model name' /proc/cpuinfo | cut -d: -f2").Output(); err == nil {
			if s := strings.TrimSpace(string(out)); s != "" {
				return s
			}
		}
	}
	return "unknown"
}
