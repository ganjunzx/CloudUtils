#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/project-config.sh

if [ -f "${PROJECT_CONF_DIR}/project-env.sh" ]; then
	. "${PROJECT_CONF_DIR}/project-env.sh"
fi

CLASSPATH="${PROJECT_CONF_DIR}"



# Add jars from hive lib directory


HADOOP=${HADOOP_HOME}/bin/hadoop

if [ ! -f "${HADOOP}" ]; then
	echo "Cannot find hadoop installation: \$HADOOP_HOME must be set.";
	exit 4;
fi

export HADOOP_HOME_WARN_SUPPRESS=true

#HIVE_LIB=${HIVE_HOME}/lib
#HIVE_CONF=${HIVE_HOME}/conf

#if [ ! -d "${HIVE_LIB}" ] || [ ! -d "${HIVE_CONF}" ]; then
#	echo "cannot find hive installation: \$HIVE_HOME must be set.";
#	exit 4;
#fi

# Add hive conf dir to classpath
#CLASSPATH=${CLASSPATH}:${HIVE_CONF}

# Add jars from hive lib directory
#for f in ${HIVE_LIB}/*.jar; do
#	if [[ ! -f $f ]]; then
#		continue;
#	fi
#	CLASSPATH=${CLASSPATH}:$f
#done

HBASE_LIB=${HBASE_HOME}/lib
HBASE_CONF=${HBASE_HOME}/conf

if [[ ! -d "${HBASE_LIB}" || ! -d "${HBASE_CONF}" ]]; then
	echo "Cannot find hbase installation: \$HBASE_HOME must be set.";
fi

CLASSPATH=${CLASSPATH}:${HBASE_CONF}

for f in ${HBASE_HOME}/*.jar; do
	if  ! echo `basename $f` | grep -i "test" >& /dev/null; then
		CLASSPATH=${CLASSPATH}:$f
	fi
done 

for f in ${HBASE_LIB}/*.jar; do
	CLASSPATH=${CLASSPATH}:$f
done

PROJECT_LIB="${PROJECT_HOME}/lib"

if [ -d "${PROJECT_LIB}" ]; then
    for f in ${PROJECT_LIB}/*.jar; do
        if [[ ! -f $f ]]; then
            continue;
        fi
        CLASSPATH=$f:${CLASSPATH}
    done
fi

#以防程序去其它地方寻找jar文件
#if find $PROJECT_HOME/lib -name project-core*.jar >& /dev/null; then
#	PROJECT_JAR=`find $PROJECT_HOME/lib -name project-core*.jar | head -1`
#fi

CLASSPATH=$PROJECT_JAR:$CLASSPATH

export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:${CLASSPATH}
exec $HADOOP jar $PROJECT_JAR "$@"

