#!/bin/bash
wget https://repo1.maven.org/maven2/org/kitesdk/kite-data-s3/1.1.0/kite-data-s3-1.1.0.jar
sudo cp kite-data-s3-1.1.0.jar /usr/lib/sqoop/lib/
sudo chmod 755 kite-data-s3-1.1.0.jar
