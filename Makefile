# Force in all deployments
.PHONY: docs

docs:
	mkdir -p docs/$(package)
	pydoc -w `find $(package) -name '*.py'`
	mv *.html docs/$(package)