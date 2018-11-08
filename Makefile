coverage:
	pytest --verbose --cov=locopy

not_integration:
	pytest --verbose --cov=locopy -m 'not integration'

integration:
	pytest --verbose --cov=locopy -m 'integration'

sphinx:
	cd docs && \
	make -f Makefile clean && \
	make -f Makefile html && \
	cd ..

ghpages:
	git checkout gh-pages && \
	cp -r docs/build/html/* . && \
	git add -u && \
	git add -A && \
	PRE_COMMIT_ALLOW_NO_CONFIG=1 git commit -m "Updated generated Sphinx documentation"
