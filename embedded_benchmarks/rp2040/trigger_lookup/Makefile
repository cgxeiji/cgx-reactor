# -----------------------------------------------------------------------
# trigger_lookup benchmark build helper
#
# Cross-compiles against pico-sdk 2.2.0, cloned on first run into
# .pico-sdk/ (git-ignored). Requires arm-none-eabi-gcc (RPi 13.2+).
# This benchmark is standalone — NOT wired into the host `make examples`.
# -----------------------------------------------------------------------

BUILD_DIR  ?= build
SDK_PATH   ?= $(CURDIR)/.pico-sdk
SDK_TAG    ?= 2.2.0
CMAKE_ARGS ?= -DPICO_SDK_PATH=$(SDK_PATH) -DCMAKE_BUILD_TYPE=Release

.PHONY: all sdk clean rebuild flash monitor

all: sdk
	cmake -B $(BUILD_DIR) -S . $(CMAKE_ARGS)
	cmake --build $(BUILD_DIR) -j$$(sysctl -n hw.ncpu 2>/dev/null || nproc)

sdk:
	@if [ ! -d $(SDK_PATH) ]; then \
		echo "Cloning pico-sdk $(SDK_TAG) into $(SDK_PATH)..."; \
		git clone --depth 1 --branch $(SDK_TAG) --recurse-submodules \
			https://github.com/raspberrypi/pico-sdk $(SDK_PATH); \
	fi

clean:
	rm -rf $(BUILD_DIR)

rebuild: clean all

# Flash via picotool (assumes BOOTSEL mode).
flash:
	@test -f $(BUILD_DIR)/trigger_lookup.uf2 || { echo "Run 'make' first."; exit 1; }
	picotool load -f $(BUILD_DIR)/trigger_lookup.uf2

# Open serial monitor (USB CDC).
monitor:
	picocom -b 115200 /dev/tty.usbmodem* 2>/dev/null || \
	screen /dev/tty.usbmodem* 115200 2>/dev/null || \
	echo "No USB serial device found. Check connection."
