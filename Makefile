.PHONY: all develop list lint release test version


PACKAGE := $(shell grep '^PACKAGE =' setup.py | cut -d "'" -f2)
VERSION := $(shell head -n 1 $(PACKAGE)/VERSION)
LEAD := $(shell head -n 1 LEAD.md)


all: list

develop:
	pip install --upgrade -e .[develop]

list:
	@grep '^\.PHONY' Makefile | cut -d' ' -f2- | tr ' ' '\n'

lint:
	pylint $(PACKAGE)

readme:
	pip install md-toc
	md_toc -p README.md github --header-levels 3
	sed -i '/(#tableschema-spss-py)/,+2d' README.md

release:
	git checkout master
	git pull origin
	git fetch -p
	git commit -a -m 'v$(VERSION)'
	git tag -a v$(VERSION) -m 'v$(VERSION)'
	git push --follow-tags

templates:
	sed -i -E "s/@(\w*)/@$(LEAD)/" .github/issue_template.md
	sed -i -E "s/@(\w*)/@$(LEAD)/" .github/pull_request_template.md

test:
	tox

version:
	@echo $(VERSION)
