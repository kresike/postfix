package postfix

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Load a map file into a memorymap
func Load(filename string) *MemoryMap {
	f, err := os.Open(filename)
	if err != nil {
		fmt.Println("opening file: ", err.Error())
		return nil
	}
	defer f.Close()
	c := 0
	s := bufio.NewScanner(f)
	res := NewMemoryMap()
	for s.Scan() {
		c++
		t := strings.Fields(s.Text())
		if len(t) != 2 {
			panic(fmt.Errorf("cannot parse file content of %s at line %d: %s", filename, c, t))
		}
		res.Add(t[0], t[1])
	}
	return res
}
