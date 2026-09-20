// Command gosched-trace turns a recorded Chrome Trace Event file back into the
// aggregate analysis a human can read.
//
// It deliberately reads the exported file rather than a live recorder: if the
// export dropped a field the analysis needs, this command cannot produce the
// same numbers, so running it is also an end-to-end check of the format.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/eigendash/gosched/trace"
)

func main() {
	in := flag.String("in", "results/example-trace.json", "Chrome Trace Event file to analyse")
	out := flag.String("out", "", "write the report here instead of stdout")
	flag.Parse()

	f, err := os.Open(*in)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gosched-trace:", err)
		os.Exit(1)
	}
	defer f.Close()

	rec, err := trace.ParseChrome(f)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gosched-trace:", err)
		os.Exit(1)
	}
	rep := trace.Analyze(rec)

	w := os.Stdout
	if *out != "" {
		file, err := os.Create(*out)
		if err != nil {
			fmt.Fprintln(os.Stderr, "gosched-trace:", err)
			os.Exit(1)
		}
		defer file.Close()
		w = file
	}
	fmt.Fprintf(w, "source: %s\n\n", *in)
	if err := rep.WriteText(w); err != nil {
		fmt.Fprintln(os.Stderr, "gosched-trace:", err)
		os.Exit(1)
	}
}
