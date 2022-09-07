# API test suite for Marqo

## Prerequisites
- Have Docker installed
- Have git clone access to the Marqo repo (everyone should as it's public)
- Have Python3.8 installed
- Have pip installed 

## Set up

1. Make a copy of `conf_sample` called `conf` in the same directory. 
Fill in the environment variables/credentials in `conf` as appropriate. 
The `conf` file will be read by the startup scripts in order to populate environment variables.