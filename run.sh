#!/bin/bash

check_status() {
    if [ $? -ne 0 ]; then
        echo "$1 failed!"
        exit 1
    fi
}

git checkout .
git pull
check_status "Update"

chmod +x run.sh

# use pip instead of apt, requires --break-system-packages 
pip install --requirement /home/ubuntu/psp-order-deltas/requirements.txt --break-system-packages 
check_status "Installation"

# Run the script
/usr/bin/python3 /home/ubuntu/psp-order-deltas/main.py
check_status "Script execution"