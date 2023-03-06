GO ?= go

CROSSBUILD := $(addprefix crossbuild-, \
	linux/386 linux/amd64 linux/arm64 linux/ppc64 linux/mips \
	darwin/amd64 darwin/arm64 \
	freebsd/arm openbsd/386 dragonfly/amd64 \
	windows/386 windows/amd64 \
)

.PHONY: $(MAKECMDGOALS) $(CROSSBUILD)

TRIMDIRS := $(shell pwd):$(shell $(GO) env GOROOT):$(shell $(GO) env GOPATH)
LD_STRIP := -gcflags=all="-trimpath=$(TRIMDIRS)" -asmflags=all="-trimpath=$(TRIMDIRS)" -ldflags=all="-s -w -buildid="

build:
	@mkdir -p bin/
	$(GO) build \
		-o bin/anelace ./cmd/anelace

build-all: build $(CROSSBUILD)

$(CROSSBUILD): %:
	@mkdir -p bin/crossbuild

	GOOS=$(patsubst crossbuild-%/,%,$(dir $*)) GOARCH=$(notdir $*) \
		$(GO) build \
		$(LD_STRIP) \
		-o bin/crossbuild/$(patsubst crossbuild-%/,%,$(dir $*))-$(notdir $*)_anelace ./cmd/anelace

test: build build-maint $(CROSSBUILD)
	@# anything above 32 and we blow through > 256 open file handles
	$(GO) test -timeout=0 -parallel=32 -count=1 -failfast ./...

build-maint:
	mkdir -p tmp/maintbin
	$(GO) build -o tmp/maintbin/dezstd ./maint/src/dezstd
