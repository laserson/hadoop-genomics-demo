import os
import sys
import json
import time
import string
import random
import subprocess

import boto
from mrjob.job import MRJob
from mrjob.conf import combine_lists, combine_dicts

def log_to_stderr(msg):
    print >>sys.stderr, msg
    sys.stderr.flush()

def unpack_datum_emr(line):
    fields = line.split('\t')
    _ = fields[0]
    value = fields[1]
    datum = json.loads(value)
    source = datum['source']
    target = datum['target']
    bucket_name = datum['bucket']
    key_name = datum['key']
    log_to_stderr(json.dumps(datum))
    return (source, target, bucket_name, key_name)

def unpack_datum_local(line):
    datum = json.loads(line)
    source = datum['source']
    target = datum['target']
    bucket_name = datum['bucket']
    key_name = datum['key']
    log_to_stderr(json.dumps(datum))
    return (source, target, bucket_name, key_name)

def exists_in_s3(bucket_name, key_name):
    log_to_stderr("boto connecting to s3")
    conn = boto.connect_s3()
    log_to_stderr("connected")
    log_to_stderr("looking up bucket: %s" % bucket_name)
    bucket = conn.lookup(bucket_name)
    if bucket is None:
        log_to_stderr("Didn't find it; creating it")
        bucket = conn.create_bucket(bucket_name)
    log_to_stderr("Have the bucket; looking up key: %s" % key_name)
    key = bucket.get_key(key_name)
    if key is not None:
        log_to_stderr("key already exists")
        return True
    else:
        log_to_stderr("key does not exist yet")
        return False

def wait_to_finish_while_reporting_progress(p):
    while True:
        print >>sys.stderr, "\nreporter:status:waiting_for_transfer"
        time.sleep(10)
        p.poll()
        if p.returncode is not None:
            log_to_stderr("Download finished with return code %s" % str(p.returncode))
            p.wait()
            return

def report_progress_callback(transmitted, size):
    print >>sys.stderr, "\nreporter:status:%i_of_%i" % (transmitted, size)
    print >>sys.stderr, "Uploaded %i of %i" % (transmitted, size)


class DownloadToS3(MRJob):
    
    HADOOP_INPUT_FORMAT = 'org.apache.hadoop.mapred.lib.NLineInputFormat'
    JOBCONF = {'mapred.line.input.format.linespermap': 1}
    
    def emr_job_runner_kwargs(self):
        args = super(DownloadToS3, self).emr_job_runner_kwargs()
        
        # set up AWS credentials on EMR instances
        access_key = os.environ['AWS_ACCESS_KEY_ID']
        secret = os.environ['AWS_SECRET_ACCESS_KEY']
        args['cmdenv'] = combine_dicts(args['cmdenv'], {'AWS_ACCESS_KEY_ID': access_key, 'AWS_SECRET_ACCESS_KEY': secret})
        
        # install pip, aws-cli, and boto
        args['bootstrap_cmds'] = combine_lists(args['bootstrap_cmds'],
                                               ['sysctl -w "net.ipv4.tcp_window_scaling=0"',
                                                'sudo apt-get install python-pip',
                                                'sudo pip install awscli',
                                                'sudo pip install boto'])
        return args
    
    def mapper(self, _, line):
        # unpack data
        (source, target, bucket_name, key_name) = unpack_datum_emr(line)
        # (source, target, bucket_name, key_name) = unpack_datum_local(line)
        
        # check if file already exists in S3
        if exists_in_s3(bucket_name, key_name):
            return None
        
        # download the source to local disk
        intermediate = ''.join(random.sample(string.ascii_letters, 10))
        log_to_stderr("Writing to local file: %s" % intermediate)
        p = subprocess.Popen('curl %s > %s' % (source, intermediate), shell=True, stdout=sys.stderr, stderr=sys.stderr)
        wait_to_finish_while_reporting_progress(p)
        log_to_stderr("Finished the download of %s to %s" % (source, intermediate))
        log_to_stderr(os.stat(intermediate))
        
        # upload to target location on S3
        p = subprocess.Popen('aws --region us-east-1 s3 cp %s %s' % (intermediate, target), shell=True, stdout=sys.stderr, stderr=sys.stderr)
        wait_to_finish_while_reporting_progress(p)
        
        return None
    
    def steps(self):
        return [self.mr(mapper=self.mapper)]

if __name__ == '__main__':
    DownloadToS3.run()