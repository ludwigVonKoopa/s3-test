
help:
	@echo "use 'make run' to start era5 processing"


run:
	@echo "run"
	bash upload_to_s3.sh

test:
	pytest test.py -v