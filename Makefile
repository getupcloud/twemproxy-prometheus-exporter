IMAGE_TAG  ?= 0.2.1
IMAGE_NAME ?= ghcr.io/getupcloud/twemproxy-prometheus-exporter
IMAGE      := $(IMAGE_NAME):$(IMAGE_TAG)

build:
	docker build . -t $(IMAGE) $(ARGS)

push:
	docker push $(IMAGE) $(ARGS)

release: build push
	git commit -am 'Build release $(IMAGE_TAG)'
	git tag $(IMAGE_TAG)
	git push origin $(IMAGE_TAG)
	git push origin main
