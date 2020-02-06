#!/bin/bash

sudo CJ_DB_URL=$CJ_DB_URL CJ_DB_PORT=$CJ_DB_PORT CJ_DB_UN=$CJ_DB_UN CJ_DB_PW=$CJ_DB_PW SESSION_SECRET=$SESSION_SECRET nohup "$(which node)" server.js &
