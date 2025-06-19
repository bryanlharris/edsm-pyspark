#!/usr/bin/env bash

(   cd /home/sqltest/edsm-data
    aws s3 sync . s3://edsm/landing/
)