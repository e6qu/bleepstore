PROJECTS := python golang rust zig

.PHONY: build-all test-unit test-e2e test-all clean-all lint-all fmt-all fmt-check-all
.PHONY: $(foreach p,$(PROJECTS),build-$(p) test-$(p) unit-$(p) e2e-$(p) clean-$(p) lint-$(p) fmt-$(p) fmt-check-$(p))

# Aggregate targets
build-all: $(foreach p,$(PROJECTS),build-$(p))
test-unit: $(foreach p,$(PROJECTS),unit-$(p))
test-e2e: $(foreach p,$(PROJECTS),e2e-$(p))
test-all: $(foreach p,$(PROJECTS),test-$(p))
clean-all: $(foreach p,$(PROJECTS),clean-$(p))
lint-all: $(foreach p,$(PROJECTS),lint-$(p))
fmt-all: $(foreach p,$(PROJECTS),fmt-$(p))
fmt-check-all: $(foreach p,$(PROJECTS),fmt-check-$(p))

# Per-project targets
define PROJECT_RULES
build-$(1):
	$$(MAKE) -C $(1) build
test-$(1):
	$$(MAKE) -C $(1) test
unit-$(1):
	$$(MAKE) -C $(1) test-unit
e2e-$(1):
	$$(MAKE) -C $(1) test-e2e
clean-$(1):
	$$(MAKE) -C $(1) clean
lint-$(1):
	$$(MAKE) -C $(1) lint
fmt-$(1):
	$$(MAKE) -C $(1) fmt
fmt-check-$(1):
	$$(MAKE) -C $(1) fmt-check
endef

$(foreach p,$(PROJECTS),$(eval $(call PROJECT_RULES,$(p))))
