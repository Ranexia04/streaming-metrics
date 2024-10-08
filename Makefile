container_name=streaming_metrics
image_name=streaming_metrics
image_version=0.0.0

main: build_go
	./${container_name} --log_level=debug --source_allow_insecure_connection=true --dest_allow_insecure_connection=true

curl:
	curl localhost:7700/metrics

run_trace: build_go
	./${container_name} --log_level=trace --source_allow_insecure_connection=true --dest_allow_insecure_connection=true

run_info: build_go
	./${container_name} --log_level=info --source_allow_insecure_connection=true --dest_allow_insecure_connection=true

build_go:
	go build -C src/main -o ../../${container_name}

pprof: build_go
	./${container_name} --pprof_on=true --log_level=debug

launch_pprof:
	/home/afonso_sr/go/bin/pprof -http=:8081 pprof/2023*.pprof

test_gojq:
	./tests/test_gojq.sh

run_container: build_cache
	podman run --rm --name ${container_name} --net host \
		-v `pwd`/metrics/:/app/metrics/:z \
		--env LOG_LEVEL=debug \
		${image_name}:${image_version}

# workaround for dockerfile context
begin_build: end_build
	mkdir -p build/gojq_extentions/
	cp -r ../gojq_extentions/go.* build/gojq_extentions/
	cp -r ../gojq_extentions/src build/gojq_extentions/src

end_build:
	rm -rf build/

build: begin_build
	echo "Building ${image_name}:${image_version} --no-cache"
	podman build -t ${image_name}:${image_version} . --no-cache
	make end_build

build_cache: begin_build
	echo "Building ${image_name}:${image_version} --with-cache"
	podman build -t ${image_name}:${image_version} .
	make end_build

docker_hub: build
	./push_dockerhub.sh ${image_name} ${image_version}

start_pulsar:
	podman run -d --rm --name pulsar -p 6650:6650 -p 8080:8080 docker.io/apachepulsar/pulsar:latest bin/pulsar standalone

clean:
	rm ${container_name}; \
	rm -r persistent_data; \
	rm pprof/2023*; \
	rm metrics/configs/TEST_*; \
	rm -r metrics/TEST_*; \
	rm -r gojq_extention

.PHONY: clean start_pulsar podman_hub build_cache build run_container test_gojq launch_pprof pprof build_go run_trace curl main
