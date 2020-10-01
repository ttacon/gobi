package gobi

import (
	"io/ioutil"

	"github.com/markbates/pkger"
)

func loadScripts() (map[string]scriptInfo, error) {
	// pkger simply walks the AST and looks for calls to open, doing
	// some magic behind the scenes. It would be preferrable to loop
	// over these, but because there's magic involved we declare these
	// things statically.
	addJobFile, err := pkger.Open("/bee-queue/lib/lua/addJob.lua")
	if err != nil {
		return nil, err
	}
	addDelayedJobFile, err := pkger.Open("/bee-queue/lib/lua/addDelayedJob.lua")
	if err != nil {
		return nil, err
	}
	checkStalledJobsFile, err := pkger.Open("/bee-queue/lib/lua/checkStalledJobs.lua")
	if err != nil {
		return nil, err
	}
	raiseDelayedJobsFile, err := pkger.Open("/bee-queue/lib/lua/raiseDelayedJobs.lua")
	if err != nil {
		return nil, err
	}
	removeJobFile, err := pkger.Open("/bee-queue/lib/lua/removeJob.lua")
	if err != nil {
		return nil, err
	}

	// Read everything into a bytes array so we can munge it into a string
	addJobBytes, err := ioutil.ReadAll(addJobFile)
	if err != nil {
		return nil, err
	}
	addDelayedJobBytes, err := ioutil.ReadAll(addDelayedJobFile)
	if err != nil {
		return nil, err
	}
	checkStalledJobsBytes, err := ioutil.ReadAll(checkStalledJobsFile)
	if err != nil {
		return nil, err
	}
	raiseDelayedJobsBytes, err := ioutil.ReadAll(raiseDelayedJobsFile)
	if err != nil {
		return nil, err
	}
	removeJobBytes, err := ioutil.ReadAll(removeJobFile)
	if err != nil {
		return nil, err
	}

	return map[string]scriptInfo{
		"addJob":           scriptInfo{3, string(addJobBytes)},
		"addDelayedJob":    scriptInfo{4, string(addDelayedJobBytes)},
		"checkStalledJobs": scriptInfo{4, string(checkStalledJobsBytes)},
		"raiseDelayedJobs": scriptInfo{2, string(raiseDelayedJobsBytes)},
		"removeJob":        scriptInfo{7, string(removeJobBytes)},
	}, nil
}

type scriptInfo struct {
	numKeys int
	raw     string
}
