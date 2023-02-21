#!/bin/bash

DEPS="python virtualenv"

# Check to ensure we have all dependent external commands to setup the runtime environment
for cmd in $DEPS; do
    which "$cmd" > /dev/null

    if [ $? -ne 0 ]; then
        echo "ERROR: ${cmd} was not found in your PATH; please install it before running this application" >&2
        exit 1
    fi
done

# Move into the directory containing the current script
CURDIR="$( cd "$( dirname "$0" )" && pwd )"
cd "$CURDIR"

# Setup the python virtual environment, if needed
if [ ! -d venv ]; then
    virtualenv venv
fi

# Activate the python virtual environment for this application
source venv/bin/activate

# Install any requirements in the virtualenv
pip install -r requirements.txt

# Execute the application
exec python main.py
