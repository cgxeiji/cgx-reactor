.PHONY: test examples example_basic example_error_handling example_member_task example_channel example_logger

test:
	cmake --build build --target cgx-reactor-tests
	ctest --test-dir build --output-on-failure

examples: example_basic example_error_handling example_member_task example_channel example_logger

example_basic:
	cmake --build build --target cgx-reactor-basic

example_error_handling:
	cmake --build build --target cgx-reactor-error-handling

example_member_task:
	cmake --build build --target cgx-reactor-member-task

example_channel:
	cmake --build build --target cgx-reactor-channel

example_logger:
	cmake --build build --target cgx-reactor-logger
