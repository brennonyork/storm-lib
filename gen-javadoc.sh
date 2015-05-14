#!/bin/bash

JAVADOC=/usr/bin/javadoc
OUTPUT_DIR=./html
SRC_PATH=./src/jvm
SIREN=org.brennonyork.siren
SIREN_EXAMPLES=org.brennonyork.siren.example
POSEIDON=org.brennonyork.poseidon
POSEIDON_ACCUMULO=org.brennonyork.poseidon.accumulo


$JAVADOC -d $OUTPUT_DIR -sourcepath $SRC_PATH $SIREN $SIREN_EXAMPLES $POSEIDON $POSEIDON_ACCUMULO
