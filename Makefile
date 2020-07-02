RUSTFLAGS                ?= "-C target-cpu=native"
MACOSX_DEPLOYMENT_TARGET ?= "10.15"

build:
	RUSTFLAGS=$(RUSTFLAGS) MACOSX_DEPLOYMENT_TARGET=$(MACOSX_DEPLOYMENT_TARGET) \
		cargo build --release

test:
	cargo test

.PHONY: build test
