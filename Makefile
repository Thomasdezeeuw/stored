export RUSTFLAGS                ?= -C target-cpu=native
export MACOSX_DEPLOYMENT_TARGET ?= 10.15
export GIT_SHORT_HASH            = $(shell git rev-parse --short HEAD)
# Either " modified" or empty if on a clean branch.
export GIT_MODIFIED              = $(shell git diff --quiet --ignore-submodules HEAD 2> /dev/null || echo " modified")
export COMMIT_VERSION            = $(GIT_SHORT_HASH)$(GIT_MODIFIED)

build:
	cargo build --release

test:
	cargo test

dev:
	find src/ tests/ | RUST_BACKTRACE=0 entr -c cargo test -q

lint:
	cargo clippy --all-targets -- --warn warnings \
		--warn clippy::correctness \
		--warn clippy::style \
		--warn clippy::complexity \
		--warn clippy::perf \
		--allow clippy::assertions_on_constants \
		--allow clippy::borrow_interior_mutable_const \
		--allow clippy::enum_variant_names \
		--allow clippy::len_without_is_empty \
		--allow clippy::needless_lifetimes \
		--allow clippy::new-without-default \
		--allow clippy::partialeq_ne_impl

clean:
	cargo clean

.PHONY: build test dev lint clean
