#!/bin/bash

mvn install:install-file -Dfile=../core/target/tachyon-0.7.0-SNAPSHOT-jar-with-dependencies.jar -DgroupId=org.tachyonproject -DartifactId=tachyon -Dversion=0.7.0-SNAPSHOT-jar-with-dependencies -Dpackaging=jar -DlocalRepositoryPath=./localRepo
