# Local development stays on the host with uv:
#   uv sync
#   uv run python main.py
#
# Build deployment artifacts for Oleander-managed Spark.
# Outputs: out/pyfiles.zip, out/environment.tar.gz

PYTHON_VERSION := 3.11
DOCKER_PLATFORM := linux/amd64
DOCKER_TARGET := artifact
OUT_DIR := out
PYFILES_NAME := pyfiles.zip
PYFILES_PATH := $(OUT_DIR)/$(PYFILES_NAME)
PYFILES_SOURCES := $(shell find mylib -type f ! -path '*/__pycache__/*' ! -name '*.pyc' | sort)
ENVIRONMENT_NAME := environment.tar.gz
ENVIRONMENT_PATH := $(OUT_DIR)/$(ENVIRONMENT_NAME)

.PHONY: all clean rebuild pyfiles environment

all: pyfiles environment
	@echo "Artifacts ready at $(PYFILES_PATH) and $(ENVIRONMENT_PATH)"

pyfiles: $(PYFILES_PATH)
	@echo "Pyfiles ready at $(PYFILES_PATH)"

environment: $(ENVIRONMENT_PATH)
	@echo "Environment ready at $(ENVIRONMENT_PATH)"

$(OUT_DIR):
	mkdir -p $(OUT_DIR)

$(PYFILES_PATH): Makefile mylib $(PYFILES_SOURCES) | $(OUT_DIR)
	rm -f $@
	zip -rq $@ mylib -x '*/__pycache__/*' '*.pyc'

$(ENVIRONMENT_PATH): Makefile Dockerfile pyproject.toml uv.lock | $(OUT_DIR)
	docker build \
		--platform $(DOCKER_PLATFORM) \
		--build-arg PYTHON_VERSION=$(PYTHON_VERSION) \
		--target $(DOCKER_TARGET) \
		--output type=local,dest=$(OUT_DIR) \
		.

clean:
	rm -rf $(OUT_DIR)

rebuild: clean all
