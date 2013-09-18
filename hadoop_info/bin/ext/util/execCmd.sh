execCmd () {
  CLASS=$1;
  shift;

  exec $PROJECT_HOME/bin/run.sh $CLASS "$@"
}
