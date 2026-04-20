.PHONY: build test lint fmt fmt-check audit clean

build:
	cargo build

test:
	cargo test

lint:
	cargo clippy -- -D warnings

fmt:
	cargo fmt

fmt-check:
	cargo fmt --check

audit:
	cargo audit

clean:
	cargo clean
