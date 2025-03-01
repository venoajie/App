
install:
	uv venv
	uv pip install -r requirements.txt
	uv python install 3.13
	uv python pin 3.13


clean:
	rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete

.PHONY: all venv run clean