from tqdm.notebook import tqdm
import pandas as pd
from io import StringIO
import boto3,time
import os
import random
import string
import subprocess
from IPython.core.magic_arguments import (argument, magic_arguments,
    parse_argstring)
from IPython.core.magic import register_line_cell_magic

build_cmds = [  
    ("Installing {package_name} ...", [
        'source /home/emr-notebook/venvs/venv-{dir_name}/bin/activate',
        'pip3 install {package_name}'
    ])
]

pre_cmds=[
    ('Initializing ...', [
        'rm -fr /home/emr-notebook/venvs/venv-{dir_name}',
        'mkdir -p /home/emr-notebook/venvs/venv-{dir_name}'
    ]),
    ('Initializing venv ...', [ 
        'virtualenv -p python3 /home/emr-notebook/venvs/venv-{dir_name}',
        'source /home/emr-notebook/venvs/venv-{dir_name}/bin/activate',
        'pip3 install --upgrade pip',
        'pip3 install venv-pack'
    ])
]

post_cmds=[
    ('Uploading venv ...', [
        'source /home/emr-notebook/venvs/venv-{dir_name}/bin/activate',
        'venv-pack -f -o /home/emr-notebook/venvs/venv-{dir_name}/venv_{dir_name}.tar.gz',
        'aws s3 cp /home/emr-notebook/venvs/venv-{dir_name}/venv_{dir_name}.tar.gz {s3_location}'
    ])
]

clean_up_cmds = [
    ('Cleaning up ...',[
        'rm -fr /home/emr-notebook/venvs/venv-{dir_name}'
    ])
]

@magic_arguments()
@argument('-v', '--verbose', action='store_true', default=False, help='enables verbose output')
@argument('s3_location', help='s3 location to upload archive')
@argument("--style","-s",help=("Add some style arguments"))
@register_line_cell_magic
def build_pyspark_dependencies(line, cell):
    """ Builds python dependencies into a virtual environment archive.

    """
    letters = string.ascii_lowercase
    dir_name = ''.join(random.choice(letters) for i in range(10)) 
    args = parse_argstring(build_pyspark_dependencies, line)
    s3_location = args.s3_location
    
    command = ";".join([c for c in cell.splitlines()])
    package_names = command.split(';')
    print ("Packages to install : "+command)
    print ("Target S3 location : "+s3_location)
    ret = subprocess.run(command, capture_output=True, shell=True)
    print(ret.stdout.decode())
    
    pbar = tqdm(pre_cmds)
    for cmd in pbar:
        pbar.set_description(cmd[0])
        exec_cmds =  " \n\t".join([c.format(dir_name=dir_name) for c in cmd[1]])
        if args.verbose: print (cmd[0]+" \n")
        if args.verbose: print ("\t"+exec_cmds)
        
        process = subprocess.Popen('/bin/bash', stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        out, err = process.communicate(exec_cmds.encode('utf-8'))
        exit_code = process.wait()
        if args.verbose: print(f'\texit_code : {exit_code} \n')
        if exit_code !=0:
            pbar.close()
            print ("\nOutput : \n\n"+(out.decode('utf-8')))
            raise Exception('Unexpected Error from command. Please check output.')
   
    for package_name in package_names:
        pbar = tqdm(build_cmds)
        for cmd in pbar:
            pbar.set_description(cmd[0].format(package_name=package_name))
            exec_cmds =  " \n\t".join([c.format(dir_name=dir_name,package_name=package_name) for c in cmd[1]])
            if args.verbose: print (cmd[0].format(package_name=package_name)+" \n")
            if args.verbose: print ("\t"+exec_cmds)

            process = subprocess.Popen('/bin/bash', stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
            out, err = process.communicate(exec_cmds.encode('utf-8'))
            exit_code = process.wait()
            if args.verbose: print(f'\texit_code : {exit_code} \n')
            if exit_code !=0:
                pbar.close()
                print ("\nUnexpected Error from command. Please check output. : \n\n"+(out.decode('utf-8')))
                return
                #raise Exception('Unexpected Error from command. Please check output.')

    pbar = tqdm(post_cmds)
    for cmd in pbar:
        pbar.set_description(cmd[0])
        exec_cmds =  " \n\t".join([c.format(dir_name=dir_name,s3_location=s3_location) for c in cmd[1]])
        if args.verbose: print (cmd[0]+" \n")
        if args.verbose: print ("\t"+exec_cmds)
        
        process = subprocess.Popen('/bin/bash', stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        out, err = process.communicate(exec_cmds.encode('utf-8'))
        exit_code = process.wait()
        if args.verbose: print(f'\texit_code : {exit_code} \n')
        if exit_code !=0:
            pbar.close()
            print ("\nOutput : \n\n"+(out.decode('utf-8')))
            raise Exception('Unexpected Error from command. Please check output.')
    
    pbar = tqdm(clean_up_cmds)
    for cmd in pbar:
        pbar.set_description(cmd[0])
        exec_cmds =  " \n\t".join([c.format(dir_name=dir_name) for c in cmd[1]])
        if args.verbose: print (cmd[0]+" \n")
        if args.verbose: print ("\t"+exec_cmds)
        
        process = subprocess.Popen('/bin/bash', stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
        out, err = process.communicate(exec_cmds.encode('utf-8'))
        exit_code = process.wait()
        if args.verbose: print(f'\texit_code : {exit_code} \n')
        if exit_code !=0:
            pbar.close()
            print ("\nOutput : \n\n"+(out.decode('utf-8')))
            raise Exception('Unexpected Error from command. Please check output.')
    
    archive_location = f"{s3_location}venv-{dir_name}/venv_{dir_name}.tar.gz"
    print (f"Archive Location : {archive_location}")
    print (f'To use in Spark, use config "spark.yarn.dist.archives":"{archive_location}"')
           