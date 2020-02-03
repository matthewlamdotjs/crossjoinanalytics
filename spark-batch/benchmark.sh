#!/bin/bash

START=$(date +"%s")

./submit.sh

END=$(date +"%s")

DIFF="$(($END-$START))"

echo Time to complete: $DIFF seconds
