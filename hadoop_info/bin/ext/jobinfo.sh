THISSERVICE=jobinfo
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

jobinfo () {
	CLASS=com.richinfo.hadoop.jobinfo.JobInfo
	execCmd $CLASS "$@"
}

jobinfo_help () {
	CLASS=com.richinfo.hadoop.jobinfo.JobInfo
	execCmd $CLASS "--help"
}
