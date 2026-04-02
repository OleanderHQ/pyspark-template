# Local development stays on the host with uv:
#   uv sync
#   uv run python main.py
#
# Build the deployment artifact with Docker for Oleander-managed Spark.
# Output: out/environment.tar.gz

PYTHON_VERSION := 3.11
DOCKER_PLATFORM := linux/amd64
DOCKER_TARGET := artifact
OUT_DIR := out
ARTIFACT_NAME := environment.tar.gz
ARTIFACT_PATH := $(OUT_DIR)/$(ARTIFACT_NAME)

.PHONY: all clean rebuild

all: $(ARTIFACT_PATH)
	@echo "Artifact ready at $(ARTIFACT_PATH)"

$(OUT_DIR):
	mkdir -p $(OUT_DIR)

$(ARTIFACT_PATH): Dockerfile pyproject.toml uv.lock | $(OUT_DIR)
	docker build \
		--platform $(DOCKER_PLATFORM) \
		--build-arg PYTHON_VERSION=$(PYTHON_VERSION) \
		--target $(DOCKER_TARGET) \
		--output type=local,dest=$(OUT_DIR) \
		.

clean:
	rm -rf $(OUT_DIR)

rebuild: clean all
