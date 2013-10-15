import json
import ftplib

def ftp_list(ftp, path):
    cmd = 'MLSD %s' % path
    lines = []
    ftp.retrlines(cmd, lines.append)
    results = []
    for line in lines:
        facts_found, _, name = line.rstrip('\r\n').partition(' ')
        entry = {}
        for fact in facts_found[:-1].split(";"):
            key, _, value = fact.partition("=")
            entry[key.lower()] = value
        results.append((name, entry))
    return results

# 1000 Genome VCF data
bucket = 'laserson-genomics'
host = 'ftp-trace.ncbi.nih.gov'
base_dir = '1000genomes/ftp/release/20110521'
ftp = ftplib.FTP(host)
ftp.login()
with open('sources/1kg.json', 'w') as op:
    for stat in ftp_list(ftp, base_dir):
        if stat[1]['type'] == 'file':
            name = stat[0]
            if name.startswith('ALL.chr') and name.endswith('.vcf.gz'):
                key = '1kg_vcf/%s' % name
                source = 'ftp://%s/%s/%s' % (host, base_dir, name)
                target = 's3://%s/%s' % (bucket, key)
                datum = {'source': source, 'target': target, 'bucket': bucket, 'key': key, 'name': name}
                print >>op, json.dumps(datum)
