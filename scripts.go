package gobi

import (
	"fmt"
	"io/ioutil"

	"github.com/markbates/pkger"
)

type scriptInfo struct {
	numKeys int
	raw     string
}

func loadScript(name string) string {
	filename := fmt.Sprintf("/bee-queue/lib/lua/%s.lua", name)

	file, err := pkger.Open(filename)
	if err != nil {
		panic(err)
	}

	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}

	return string(bytes)
}

func loadScripts() (map[string]scriptInfo, error) {
	pkger.Include("/bee-queue/lib/lua")

	return map[string]scriptInfo{
		"addJob":           scriptInfo{3, loadScript("addJob")},
		"addDelayedJob":    scriptInfo{4, loadScript("addDelayedJob")},
		"checkStalledJobs": scriptInfo{4, loadScript("checkStalledJobs")},
		"raiseDelayedJobs": scriptInfo{2, loadScript("raiseDelayedJobs")},
		"removeJob":        scriptInfo{7, loadScript("removeJob")},
	}, nil
}
