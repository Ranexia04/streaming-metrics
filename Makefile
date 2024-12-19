container_name=streaming-metrics
image_name=docker.io/xcjsbsx/streaming-metrics
image_tag=0.1.3

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
	go tool pprof -http=:8081 ./pprof/*.pprof

test_gojq:
	./tests/test_gojq.sh

run_container: build_cache
	podman run --rm --name ${container_name} --net host \
		-v ./namespaces/:/app/namespaces/:z \
		-v ./groups/:/app/groups/:z \
		-v ./filters/:/app/filters/:z \
		-v ./pprof/:/app/pprof/:z \
		--env LOG_LEVEL=info \
		--env PPROF_ON=true \
		--env CONSUMER_THREADS=6 \
		--env GRANULARITY=15 \
		--env CARDINALITY=2 \
		${image_name}:${image_tag}

# workaround for dockerfile context
begin_build: end_build
	mkdir -p build/gojq_extentions/
	cp -r ../gojq_extentions/go.* build/gojq_extentions/
	cp -r ../gojq_extentions/src build/gojq_extentions/src

end_build:
	rm -rf build/

build: begin_build
	echo "Building ${image_name}:${image_tag} --no-cache"
	podman build -t ${image_name}:${image_tag} . --no-cache
	make end_build

build_cache: begin_build
	echo "Building ${image_name}:${image_tag} --with-cache"
	podman build -t ${image_name}:${image_tag} .
	make end_build

docker_hub: build
	./push_dockerhub.sh ${image_name} ${image_tag}

start_pulsar:
	podman run -it -p 6650:6650 -d --name pulsar -p 8080:8080 -p 8000:8000 docker.io/apachepulsar/pulsar:latest bin/pulsar standalone
#  --volume pulsardata:/pulsar/data:z --volume pulsarconf:/pulsar/conf:z
clean:
	rm ${container_name}; \
	rm pprof/*; \
	rm -r namespaces \
	rm -r groups \
	rm -r filters \
	rm -r gojq_extention

.PHONY: clean start_pulsar podman_hub build_cache build run_container test_gojq launch_pprof pprof build_go run_trace curl main
