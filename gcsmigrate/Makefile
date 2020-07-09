function: _function.tmp_dir function.tmp/requirements.txt
	cp shared.py main.py function.tmp/
	cd function.tmp && zip -r ../function.zip *

_function.tmp_dir:
	mkdir -p function.tmp

function.tmp/requirements.txt:
	pipenv lock -r > function.tmp/requirements.txt

clean:
	rm -rf function.tmp function.zip requirements.txt
