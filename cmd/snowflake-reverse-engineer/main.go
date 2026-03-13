// Command snowflake-reverse-engineer parses CLI flags, connects to Snowflake,
// and writes one SQL file per INFORMATION_SCHEMA view plus storage integrations.
package main

import "os"

func main() {
	os.Exit(run(os.Args[1:]))
}
