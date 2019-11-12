lambda: _lambda.tmp_dir lambda.tmp/requirements.txt
	cp *.py lambda.tmp/
	cd lambda.tmp && zip -r ../lambda.zip *

_lambda.tmp_dir:
	mkdir -p lambda.tmp

lambda.tmp/requirements.txt:
	pipenv lock -r | grep -v docutils |grep -v boto > lambda.tmp/requirements.txt
	pip install -r lambda.tmp/requirements.txt --no-deps -t lambda.tmp

clean:
	rm -rf lambda.tmp lambda.zip requirements.txt
