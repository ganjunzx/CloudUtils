#!/usr/bin/env bash

this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# the root of the Hive installation
if [[ -z $PROJECT_HOME ]] ; then
  export PROJECT_HOME=`dirname "$bin"`
fi

# Allow alternate conf dir location.
PROJECT_CONF_DIR="${PROJECT_CONF_DIR:-$PROJECT_HOME/conf}"

export PROJECT_CONF_DIR=$PROJECT_CONF_DIR

# Default to use 256MB
export HADOOP_HEAPSIZE=${HADOOP_HEAPSIZE:-256}
