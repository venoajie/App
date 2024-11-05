
install:
uv venv
uvinit pip install -r requirements.txt


clean:
	rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete

.PHONY: all venv run clean