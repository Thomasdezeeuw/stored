RUSTFLAGS                ?= "-C target-cpu=native"
MACOSX_DEPLOYMENT_TARGET ?= "10.15"

build:
	RUSTFLAGS=$(RUSTFLAGS) MACOSX_DEPLOYMENT_TARGET=$(MACOSX_DEPLOYMENT_TARGET) \
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
