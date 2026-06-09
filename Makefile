.PHONY: test example_basic example_error_handling example_member_task

test:
	cmake --build build --target cgx-reactor-tests
	ctest --test-dir build --output-on-failure

example_basic:
	cmake --build build --target cgx-reactor-basic

example_error_handling:
	cmake --build build --target cgx-reactor-error-handling

example_member_task:
	cmake --build build --target cgx-reactor-member-task
