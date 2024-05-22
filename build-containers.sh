#!/bin/bash

cd kafka_app
docker build -t kafkaapp .

cd ../casapp
docker build -t casapp .

cd ../wiki_endp
docker build -t wikiapp .
