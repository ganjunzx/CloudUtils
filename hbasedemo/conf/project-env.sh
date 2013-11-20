#以防程序去其它地方寻找jar文件
if find $PROJECT_HOME/lib -name paic-hbasedemo*.jar >& /dev/null; then
       export PROJECT_JAR=`find $PROJECT_HOME/lib -name paic-hbasedemo*.jar | head -1`
 fi

export HADOOP_HOME=${HADOOP_HOME:-/home/ganjun/soft/huge_svn/hugetable-5.0-dev/hadoop-1.0.4}
