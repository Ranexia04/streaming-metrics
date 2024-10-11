package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	gojq_extentions "example.com/gojq_extentions/src"
	"example.com/streaming-metrics/src/flow"

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

// def log($namespace; $time; $metric): {"namespace": $namespace, "time": $time, "metrics": $metrics};
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

func loadNamespaceFunc(namespaces map[string]*flow.Namespace) fs.WalkDirFunc {
	return func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		buf, err := os.ReadFile(path)
		if err != nil {
			logrus.Panicf("Unable to read file %s: %+v", path, err)
		}

		namespace := flow.NewNamespace(buf)
		if namespace == nil {
			logrus.Panicf("Unable to create namespace for file %s", path)
		}
		namespaces[namespace.Name] = namespace

		return nil
	}
}

func loadNamespaces(namespacesDir string) map[string]*flow.Namespace {
	namespaces := make(map[string]*flow.Namespace)

	err := filepath.WalkDir(namespacesDir, loadNamespaceFunc(namespaces))
	if err != nil {
		logrus.Panicf("Error walking the path %q: %v\n", namespacesDir, err)
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
	group_filter_jq_path := fmt.Sprintf("%s/%s", groupsDir, "groups.jq")
	group_filter := loadJq(group_filter_jq_path, withFunctionGroupFilterError(), withFunctionCompileTest())
	if group_filter == nil {
		logrus.Panicf("loadGroupFilters no group filter")
		return nil
	}

	return flow.NewFilterTree(group_filter)
}
