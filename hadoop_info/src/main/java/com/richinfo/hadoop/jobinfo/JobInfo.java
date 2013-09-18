package com.richinfo.hadoop.jobinfo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.JobStatus;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: ganjun
 * Date: 9/17/13
 * Time: 5:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class JobInfo {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            JobClient jobClient = new JobClient(conf);
            JobStatus[] jobStatuses = jobClient.getAllJobs();

            System.out.println("The job's numbers is :" + jobStatuses.length);
            for(JobStatus jobStatus : jobStatuses) {
                if (!jobStatus.isJobComplete()) {
                    System.out.println("job " + jobStatus.getJobName() + " Scheduler info :" + jobStatus.getSchedulingInfo());
                    System.out.println("job " + jobStatus.getJobName() + " Map Progress :" + jobClient.getJob((JobID) jobStatus.getJobID()).mapProgress());
                    System.out.println("job " + jobStatus.getJobName() + " Reduce Progress :" + jobStatus.getReduceProgress());
                    System.out.println("job " + jobStatus.getJobName() + " Start time :" + jobStatus.getStartTime());
                    System.out.println("job " + jobStatus.getJobName() + " Counters :" +
                            jobClient.getJob((JobID) jobStatus.getJobID()).getCounters().toString());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
}
