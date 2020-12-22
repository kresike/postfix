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
	s := bufio.NewScanner(f)
	res := NewMemoryMap()
	for s.Scan() {
		t := strings.Fields(s.Text())
		res.Add(t[0], t[1])
	}
	return res
}
