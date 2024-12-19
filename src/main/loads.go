package main

import (
	"fmt"
	"os"
	"path/filepath"

	gojq_extentions "example.com/gojq_extentions/src"
	"example.com/streaming-metrics/src/flow"
	"example.com/streaming-metrics/src/prom"

	"github.com/itchyny/gojq"
	"github.com/sirupsen/logrus"
)

func withFunctionNamespaceFilterError() gojq.CompilerOption {
	return gojq.WithFunction("filter_error", 1, 1, func(in any, args []any) any {
		return fmt.Errorf("filter_error: not relevant msg for namespace: %s", args[0])
	})
}

func withFunctionGroupFilterError() gojq.CompilerOption {
	return gojq.WithFunction("filter_error", 1, 1, func(in any, args []any) any {
		return fmt.Errorf("filter_error: not relevant msg for group: %s", args[0])
	})
}

func withFunctionLog() gojq.CompilerOption {
	return gojq.WithFunction("log", 3, 3, func(in any, args []any) any {
		return map[string]any{
			"namespace": args[0],
			"time":      args[1],
			"metrics":   args[2],
		}
	})
}

func withFunctionCompileTest() gojq.CompilerOption {
	return gojq.WithFunction("ctest", 1, 1, gojq_extentions.Compiled_test)
}

func loadJq(program_file string, options ...gojq.CompilerOption) *gojq.Code {
	buf, err := os.ReadFile(program_file)

	if err != nil {
		logrus.Errorf("loadJq readfile %s: %+v", program_file, err)
		return nil
	}

	program, err := gojq.Parse(string(buf))
	if err != nil {
		logrus.Errorf("loadJq parse %s: %+v", program_file, err)
		return nil
	}

	compiled_program, err := gojq.Compile(program, options...)
	if err != nil {
		logrus.Errorf("loadJq compile %s: %+v", program_file, err)
		return nil
	}

	return compiled_program
}

func loadNamespaces(namespacesDir string, granularity int64, cardinality int64, shift int64) map[string]*flow.Namespace {
	namespaces := make(map[string]*flow.Namespace)

	entries, err := os.ReadDir(namespacesDir)
	if err != nil {
		logrus.Panicf("failed to read directory %s", namespacesDir)
	}

	for _, entry := range entries {
		namespacePath := filepath.Join(namespacesDir, entry.Name())

		if entry.IsDir() {
			logrus.Infof("ignoring %v directory", namespacePath)
			continue
		}

		info, err := entry.Info()
		if err != nil {
			logrus.Errorf("failed to get info for %s: %v", namespacePath, err)
			continue
		}

		if info.Mode()&os.ModeSymlink != 0 {
			resolvedPath, err := filepath.EvalSymlinks(namespacePath)
			if err != nil {
				logrus.Errorf("failed to resolve symlink for %s: %v", namespacePath, err)
				continue
			}

			resolvedInfo, err := os.Lstat(resolvedPath)
			if err != nil {
				logrus.Errorf("failed to get info for resolved path %s: %v", resolvedPath, err)
				continue
			}

			if resolvedInfo.IsDir() {
				logrus.Infof("ignoring %v directory", namespacePath)
				continue
			}
		}

		buf, err := os.ReadFile(namespacePath)
		if err != nil {
			logrus.Panicf("Unable to read file %s: %+v", namespacePath, err)
		}

		namespace := flow.NewNamespace(buf, granularity, cardinality, shift)
		if namespace == nil {
			logrus.Panicf("Unable to create namespace for file %s", namespacePath)
		}
		namespaces[namespace.Name] = namespace
	}

	return namespaces
}

func loadFilters(filtersDir string, groupsDir string, namespaces map[string]*flow.Namespace) *flow.FilterRoot {
	filters := loadGroupFilters(groupsDir)
	for _, namespace := range namespaces {
		group := filters.GetGroup(namespace.Group)
		if group == nil {
			group = flow.NewGroupNode(namespace.Group)
			filters.AddGroup(namespace.Group, group)
			prom.IncNumberGroups()
		}

		filterJqPath := fmt.Sprintf("%s/%s.jq", filtersDir, namespace.Name)
		if filter := loadJq(filterJqPath, withFunctionNamespaceFilterError(), withFunctionLog(), withFunctionCompileTest()); filter != nil {
			group.AddChild(&flow.LeafNode{
				Filter: filter,
			})
		}
	}

	return filters
}

func loadGroupFilters(groupsDir string) *flow.FilterRoot {
	groupFilterJqPath := fmt.Sprintf("%s/%s", groupsDir, "groups.jq")
	groupFilter := loadJq(groupFilterJqPath, withFunctionGroupFilterError(), withFunctionCompileTest())
	if groupFilter == nil {
		logrus.Panicf("loadGroupFilters no group filter")
		return nil
	}

	return flow.NewFilterTree(groupFilter)
}
