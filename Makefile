# Test options.
# You can also use `TEST_FLAGS` to set additonal flags, e.g. `--release`.
TEST_OPTS = -- --quiet -Z unstable-options --shuffle
# Command to run in `dev` target, e.g. `make RUN=check dev`.
RUN ?= test

build:
	cargo build --release -C target-cpu=native

# Development loop, runs $RUN whenever a source file changes.
dev:
	find src/ tests/ examples/ Makefile Cargo.toml | entr -d -c $(MAKE) $(RUN)

test:
	cargo test --all-features $(TEST_FLAGS) $(TEST_OPTS)

test_sanitizers:
	$(MAKE) test_sanitizer sanitizer=address
	$(MAKE) test_sanitizer sanitizer=leak
	$(MAKE) test_sanitizer sanitizer=memory
	$(MAKE) test_sanitizer sanitizer=thread

# Run with `make test_sanitizer sanitizer=$sanitizer`, or use `test_sanitizers`.
test_sanitizer:
	RUSTDOCFLAGS=-Zsanitizer=$(sanitizer) RUSTFLAGS=-Zsanitizer=$(sanitizer) \
	cargo test -Zbuild-std --all-features --target x86_64-unknown-linux-gnu $(TEST_FLAGS) $(TEST_OPTS)

# TODO: add TEST_OPTS to this, currently this doesn't work with miri.
test_miri:
	cargo miri test --all-features $(TEST_FLAGS)

check:
	cargo check --all-features --all-targets

# Reasons to allow lints:
# `cargo-common-metadata`: for `benches` and `tools`.
# `doc-markdown`: too many false positives.
# `equatable-if-let`: bad lint.
# `future-not-send`: we don't want to require all generic parameters to be `Send`.
# `if-not-else`: let the logic speak for itself.
# `match-bool`: often less lines of code and I find that use `match` generally
# strictly better then `if`s.
# `missing-const-for-fn`: See https://github.com/rust-lang/rust-clippy/issues/4979.
# `module-name-repetitions`: we re-export various names.
# `needless-lifetimes`: lifetime serves as documentation.
# `option-if-let-else`: not idiomatic at all.
# `use-self`: this is a bad lint.
#
# Could fix:
# `cast-possible-truncation`.
lint: clippy
clippy:
	cargo clippy --all-features -- \
		--deny clippy::all \
		--deny clippy::correctness \
		--deny clippy::style \
		--deny clippy::complexity \
		--deny clippy::perf \
		--deny clippy::pedantic \
		--deny clippy::nursery \
		--deny clippy::cargo \
		--allow clippy::cargo-common-metadata \
		--allow clippy::cast-possible-truncation \
		--allow clippy::doc-markdown \
		--allow clippy::equatable-if-let \
		--allow clippy::future-not-send \
		--allow clippy::if-not-else \
		--allow clippy::len-without-is-empty \
		--allow clippy::match-bool \
		--allow clippy::match-same-arms \
		--allow clippy::missing-const-for-fn \
		--allow clippy::missing-errors-doc \
		--allow clippy::missing-panics-doc \
		--allow clippy::module-name-repetitions \
		--allow clippy::must-use-candidate \
		--allow clippy::needless-lifetimes \
		--allow clippy::new-without-default \
		--allow clippy::option-if-let-else \
		--allow clippy::struct-field-names \
		--allow clippy::use-self \

doc:
	cargo doc --all-features

doc_private:
	cargo doc --all-features --document-private-items

clean:
	cargo clean

.PHONY: dev test test_sanitizers test_sanitizer test_miri check lint clippy doc doc_private clean
