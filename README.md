Hadoop Genomics Demo
====================

Demo of using Hadoop ecosystem (including Impala) on genome variant data


AWS util
--------

Using keypair `laserson-genomics`.  (Ensure permission on .pem file are 600.)

Using security group `laserson`.

Adding my current IP to ingress for security group:

    curl -s http://checkip.amazonaws.com/
    aws ec2 authorize-security-group-ingress --group-name laserson --protocol tcp --port 22 --cidr `curl -s http://checkip.amazonaws.com/`/32

Set up tiny instance to work on:
    
    # Ubuntu 12.04 AMI
    aws ec2 run-instances --image-id ami-d0f89fb9 --count 1 --instance-type t1.micro --key-name laserson-genomics --security-groups laserson

Describe my instances:

    aws ec2 describe-instances --filter "Name=key-name,Values=laserson-genomics"

Login to instance:

    ssh -i ~/.ssh/laserson-genomics.pem ubuntu@[public_hostname]

Terminate instance:

    aws ec2 terminate-instances --instance-ids i-f427d98f

Tunnel to instance for JobTracker:

    ssh -i ~/.ssh/laserson-genomics.pem -L 9100:[host]:9100 hadoop@[host]


Install Aspera:

    wget http://downloads.asperasoft.com/download/sw/connect/3.1/aspera-connect-3.1.1.70545-linux-64.tar.gz
    tar -xzf aspera-connect-3.1.1.70545-linux-64.tar.gz
    sh aspera-connect-3.1.1.70545-linux-64.sh
    
    

Data ingest into S3
-------------------

This script will generate files that have lists of URLs for ingest.

    python bin/data_urls.py
    aws s3 cp sources/1kg.json s3://laserson-genomics/sources/

Download 1000 Genome:

    python bin/download_to_s3.py -r emr \
        --ec2-instance-type cc2.8xlarge \
        --num-ec2-instances 4 \
        --ec2-key-pair laserson-genomics\
        --ec2-key-pair-file ~/.ssh/laserson-genomics.pem \
        --ssh-tunnel-to-job-tracker \
        s3://laserson-genomics/sources/1kg.json

Download dbSNP from here with Aspera:

    ftp://ftp.ncbi.nih.gov/snp/organisms/human_9606/VCF/00-All.vcf.gz
    aws s3 cp 00-All.vcf.gz s3://laserson-genomics/dbSNP_raw/

Download COSMIC:

    wget ftp://ngs.sanger.ac.uk/production/cosmic/CosmicCodingMuts_v66_20130725.vcf.gz
    wget ftp://ngs.sanger.ac.uk/production/cosmic/CosmicNonCodingVariants_v66_20130725.vcf.gz
    aws s3 cp CosmicCodingMuts_v66_20130725.vcf.gz s3://laserson-genomics/COSMIC_raw/
    aws s3 cp CosmicNonCodingVariants_v66_20130725.vcf.gz s3://laserson-genomics/COSMIC_raw/