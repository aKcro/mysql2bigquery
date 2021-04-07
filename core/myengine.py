# -*- coding: utf-8 -*-
import uuid
import subprocess
import os
import settings
import sys

def dump_query(dbname, query):
    """
        There is multiple ways of doing this dump, in this case mysqlcli will be used because we are trying to avoid
        permission issues
    """
    filename = str(uuid.uuid4()) + ".csv"

    cmd = """mysql -e "{query}" -h{host} -u{user} -p{password} {dbname} | sed 's/\\t/","/g;s/^/"/;s/$/"/;s/\\n//g' > {output}"""
    cmd = cmd.format(
            query=query,
            host=settings.dbhost,
            user=settings.dbuser,
            password=settings.dbpass,
            dbname=dbname,
            output=os.path.join(settings.dumps, filename)
        )
    try:
        print("Dumping data...")
        proc = subprocess.Popen(
            cmd,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        proc.wait()
        stdout_value, stderr_value = proc.communicate()
        
        #there is something wrong in the query
        if len(stdout_value) != 0:
            print(stdout_value)
            sys.exit(0)

    except Exception as e:
        print(e)

    return filename