SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:

ifeq ($(origin .RECIPEPREFIX), undefined)
  $(error This Make does not support .RECIPEPREFIX. Please use GNU Make 4.0 or later)
endif
.RECIPEPREFIX = >

T = $(wildcard test-*.sh)

all: $(T)
.PHONY: all

clean:
> -rm -rf test-results
.PHONY: clean

$(T):
> ./$@
.PHONY: $(T)
