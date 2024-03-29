SHELL := /bin/bash
.SHELLFLAGS := -e -O xpg_echo -o errtrace -o functrace -c
MAKEFLAGS += --no-builtin-rules
MAKEFLAGS += --no-builtin-variables
MAKE := $(make)
DATETIME_FORMAT := %(%Y-%m-%d %H:%M:%S)T
.ONESHELL:
.SUFFIXES:
.DELETE_ON_ERROR:


# Targets for printing help:
.PHONY: help
help:  ## Prints this usage.
	@echo '== Recipes ==' && grep --no-filename -E '^[a-zA-Z0-9-]+:' $(MAKEFILE_LIST) | sed -E 's/:[^#]*#\s*(.*$$)/ -- \1/g' | grep '#' | tr -d '#' && \
		
		echo && \
		echo '==Info==' && \
		printf "VIRTUAL_ENV: " && \
		[ -n "$(VIRTUAL_ENV)" ] && { echo "$(VIRTUAL_ENV)"; } || { echo '<NONE>'; }


		
# see https://www.gnu.org/software/make/manual/html_node/Origin-Function.html
MAKEFILE_ORIGINS := \
	default \
	environment \
	environment\ override \
	file \
	command\ line \
	override \
	automatic \
	\%

PRINTVARS_MAKEFILE_ORIGINS_TARGETS += \
	$(patsubst %,printvars/%,$(MAKEFILE_ORIGINS)) \

.PHONY: $(PRINTVARS_MAKEFILE_ORIGINS_TARGETS)
$(PRINTVARS_MAKEFILE_ORIGINS_TARGETS):
	@$(foreach V, $(sort $(.VARIABLES)), \
		$(if $(filter $(@:printvars/%=%), $(origin $V)), \
			$(info $V=$($V) ($(value $V)))))

.PHONY: printvars
printvars: printvars/file # Print all Makefile variables (file origin).

.PHONY: printvar-%
printvar-%: # Print one Makefile variable.
	@echo '($*)'
	@echo '  origin = $(origin $*)'
	@echo '  flavor = $(flavor $*)'
	@echo '   value = $(value  $*)'


.DEFAULT_GOAL := help

## Package targets

BIDS_SPECIFICATION_REPO := https://github.com/bids-standard/bids-specification

DEFAULT_BIDS_VERSION_PATH := src/s3bids/_default_bids_version.py

GIT := $(shell command -v git 2> /dev/null)

$(DEFAULT_BIDS_VERSION_PATH): # default bids version
ifeq ($(GIT),)
	$(error "no git")
endif
	$(GIT) ls-remote --sort=-version:refname --refs --tags \
		$(BIDS_SPECIFICATION_REPO)  'v*' | \
		head -n 1 | sed 's/^.*\/v//g; s/$$/"/; s/^/DEFAULT_BIDS_VERSION="/' > $@ && \
		echo "Wrote BIDS version to $(DEFAULT_BIDS_VERSION_PATH) from $(BIDS_SPECIFICATION_REPO)" && \
		cat $(DEFAULT_BIDS_VERSION_PATH)

.PHONY: write-default-bids-version
write-default-bids-version: $(DEFAULT_BIDS_VERSION_PATH) ## Write default BIDS version.



