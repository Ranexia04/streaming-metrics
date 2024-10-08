package main

import (
	"fmt"
	"os"

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

func loadNamespaces(metricsDir string) map[string]*flow.Namespace {
	files, err := os.ReadDir(metricsDir + "/configs/")
	if err != nil {
		logrus.Panicf("load_configs unable to open directory %s %+v", metricsDir+"/configs/", err)
	}

	namespaces := make(map[string]*flow.Namespace)
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		buf, err := os.ReadFile(metricsDir + "/configs/" + file.Name())
		if err != nil {
			logrus.Panicf("Unable to read file %s: %+v", file.Name(), err)
		}

		namespace := flow.NewNamespace(buf)
		if namespace == nil {
			logrus.Panicf("Unable to create namespace for file %s", file.Name())
		}
		namespaces[namespace.Name] = namespace
	}

	return namespaces
}

func loadFilters(metricsDir string, configs map[string]*flow.Namespace) *flow.FilterRoot {
	filters := loadGroupFilters(metricsDir)
	for _, namespace := range configs {
		group := filters.GetGroup(namespace.Group)
		if group == nil {
			group = flow.NewGroupNode(namespace.Group)
			filters.AddGroup(namespace.Group, group)
		}

		filterJqPath := fmt.Sprintf("%s/%s/%s", metricsDir, namespace.Name, "filter.jq")
		if filter := loadJq(filterJqPath, withFunctionNamespaceFilterError(), withFunctionLog(), withFunctionCompileTest()); filter != nil {
			group.AddChild(&flow.LeafNode{
				Filter: filter,
			})
		}
	}

	return filters
}

func loadGroupFilters(metricsDir string) *flow.FilterRoot {
	group_filter_jq_path := fmt.Sprintf("%s/%s/%s", metricsDir, "groups", "groups.jq")
	group_filter := loadJq(group_filter_jq_path, withFunctionGroupFilterError(), withFunctionCompileTest())
	if group_filter == nil {
		logrus.Panicf("loadGroupFilters no group filter")
		return nil
	}

	return flow.NewFilterTree(group_filter)
}
