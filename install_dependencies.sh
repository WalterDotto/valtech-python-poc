#!/bin/bash

# Instalar Python
sudo apt update
sudo apt install -y python3 python3-pip

# Instalar las dependencias
pip3 install boto3
