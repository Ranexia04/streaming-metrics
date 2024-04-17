package main

import (
	"fmt"
	"os"

	gojq_extentions "example.com/gojq_extentions/src"
	"example.com/streaming_monitors/src/flow"

	"github.com/itchyny/gojq"
	"github.com/sirupsen/logrus"
)

func with_function_namespace_filter_error() gojq.CompilerOption {
	return gojq.WithFunction("filter_error", 1, 1, func(in any, args []any) any {
		return fmt.Errorf("filter_error: not relevant msg for namespace: %s", args[0])
	})
}

func with_function_group_filter_error() gojq.CompilerOption {
	return gojq.WithFunction("filter_error", 1, 1, func(in any, args []any) any {
		return fmt.Errorf("filter_error: not relevant msg for group: %s", args[0])
	})
}

// def log($namespace; $id; $time; $metric): {"namespace": $namespace, "id": $id, "time": $time, "metric": $metric};
func with_function_log() gojq.CompilerOption {
	return gojq.WithFunction("log", 4, 4, func(in any, args []any) any {
		return map[string]any{
			"namespace": args[0],
			"id":        args[1],
			"time":      args[2],
			"metric":    args[3],
		}
	})
}

func with_function_compile_test() gojq.CompilerOption {
	return gojq.WithFunction("ctest", 1, 1, gojq_extentions.Compiled_test)
}

func load_jq(program_file string, options ...gojq.CompilerOption) *gojq.Code {
	buf, _ := os.ReadFile(program_file)

	program, err := gojq.Parse(string(buf))
	if err != nil {
		logrus.Errorf("load_jq parse %s: %+v", program_file, err)
		return nil
	}

	compiled_program, err := gojq.Compile(program, options...)
	if err != nil {
		logrus.Errorf("load_jq compile %s: %+v", program_file, err)
		return nil
	}

	return compiled_program
}

func load_configs(monitors_dir string) []flow.Namespace {
	files, err := os.ReadDir(monitors_dir + "/configs/")
	if err != nil {
		logrus.Panicf("load_configs unable to open directory %s %+v", monitors_dir+"/configs/", err)
	}
	namespaces := make([]flow.Namespace, 0, len(files))

	for _, file := range files {
		if !file.IsDir() {
			buf, _ := os.ReadFile(monitors_dir + "/configs/" + file.Name())

			if namespace := flow.New_namesapce(buf); namespace != nil {
				namespaces = append(namespaces, *namespace)
			} else {
				logrus.Errorf("Unable to create namespace for file %s", file.Name())
			}
		}
	}

	return namespaces
}

func load_namespaces(monitors_dir string, configs []flow.Namespace) map[string]*flow.Namespace {
	namespaces := make(map[string]*flow.Namespace)
	for i := 0; i < len(configs); i++ {
		namespace := &configs[i]

		path_monitor_jq := fmt.Sprintf("%s/%s/%s", monitors_dir, namespace.Namespace, "monitor.jq")
		monitor := load_jq(path_monitor_jq)

		path_lambda_jq := fmt.Sprintf("%s/%s/%s", monitors_dir, namespace.Namespace, "lambda.jq")
		lambda := load_jq(path_lambda_jq, gojq.WithVariables([]string{"$state", "$metric"}), with_function_compile_test())

		if monitor != nil && lambda != nil {
			namespace.Set_monitor(monitor)
			namespace.Set_lambda(lambda)
			namespaces[namespace.Namespace] = namespace
		}

	}
	return namespaces
}

func load_filters(monitors_dir string, configs []flow.Namespace) *flow.Filter_root {
	filters := load_group_filters(monitors_dir, configs)
	for i := 0; i < len(configs); i++ {
		namespace := &configs[i]
		group := filters.Get_group(namespace.Group)
		if group == nil {
			logrus.Errorf("load_filters: Failed to load namespace %s because group %s does not exist", namespace.Namespace, namespace.Group)
			continue
		}
		path_filter_jq := fmt.Sprintf("%s/%s/%s", monitors_dir, namespace.Namespace, "filter.jq")
		if filter := load_jq(path_filter_jq, with_function_namespace_filter_error(), with_function_log(), with_function_compile_test()); filter != nil {
			group.Add_child(&flow.Leaf_node{
				Filter: filter,
			})
		}
	}
	return filters
}

func load_group_filters(monitors_dir string, configs []flow.Namespace) *flow.Filter_root {
	root := flow.New_filter_tree()
	for i := 0; i < len(configs); i++ {
		namespace := &configs[i]
		if root.Has_group(namespace.Group) {
			continue
		}
		path_group_filter_jq := fmt.Sprintf("%s/%s/%s", monitors_dir, "groups", namespace.Group+".jq")
		if group_filter := load_jq(path_group_filter_jq, with_function_group_filter_error(), with_function_compile_test()); group_filter != nil {
			group := flow.New_group_node(namespace.Group, group_filter)
			root.Add_group(namespace.Group, group)
		}
	}
	return root
}
