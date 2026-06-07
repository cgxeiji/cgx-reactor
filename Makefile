.PHONY: test example_basic

test:
	cmake --build build --target cgx-reactor-tests
	ctest --test-dir build --output-on-failure

example_basic:
	cmake --build build --target cgx-reactor-basic
