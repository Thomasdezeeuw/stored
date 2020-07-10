RUSTFLAGS                ?= "-C target-cpu=native"
MACOSX_DEPLOYMENT_TARGET ?= "10.15"

build:
	RUSTFLAGS=$(RUSTFLAGS) MACOSX_DEPLOYMENT_TARGET=$(MACOSX_DEPLOYMENT_TARGET) \
		cargo build --release

test:
	cargo test

dev:
	find src/ tests/ | RUST_BACKTRACE=0 entr -c cargo test -q

.PHONY: build test dev
