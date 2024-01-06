PWD := $(shell pwd)
all: run

run:
	@pipenv run $(shell dirname $PWD)/bot.py
