IMAGE_TAG  ?= 0.1.0
IMAGE_NAME ?= ghcr.io/getupcloud/twemproxy-prometheus-exporter
IMAGE      := $(IMAGE_NAME):$(IMAGE_TAG)

build:
	docker build . -t $(IMAGE) $(ARGS)

push:
	docker push $(IMAGE) $(ARGS)

release: build push
	git tag $(IMAGE_TAG)
	git push origin $(IMAGE_TAG)
	git push origin main
