from tqdm.notebook import tqdm
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
@register_line_cell_magic
def build_pyspark_virtualenv(line, cell):
    """ Builds python dependencies into a virtual environment archive.
    
       This function lets you specify python packages, builds a virtual
       environment archive after installing the packages using pip commands, 
       uploads the archive to the specified S3 location. It shows the 
       installation progess of each package and in case of any error, 
       prints the error message and aborts the build.
    
        %%build_pyspark_virtualenv s3_location [-v|--verbose]
        
        if -v or --verbose is passed, then you can see the commands being executed.
        
        Examples
        --------
        %%build_pyspark_virtualenv s3://mybucket/venv_archives/
        pmdarima==1.8.5
        # sample comment
        pandas==1.2.5
        matplotlib==3.5.0
        

    """
    # generate random directory name
    letters = string.ascii_lowercase
    dir_name = ''.join(random.choice(letters) for i in range(10)) 
    
    # read S3 location
    args = parse_argstring(build_pyspark_virtualenv, line)
    s3_location = args.s3_location
    
    # read package ignoring comments
    command = ";".join([c for c in cell.splitlines() if not c.startswith("#")])
    package_names = command.split(';')
    print ("Packages to install : "+command)
    print ("Target S3 location : "+s3_location)
    
    # Run pre-build commands
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
            print ("\nUnexpected Error from command. Please check output: \n\n"+(out.decode('utf-8')))
            return
   
    for package_name in package_names:
        # Run build commands
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
                print ("\nUnexpected Error from command. Please check output: \n\n"+(out.decode('utf-8')))
                return
            
    # Run post build commands
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
            print ("\nUnexpected Error from command. Please check output: \n\n"+(out.decode('utf-8')))
            return
    
    # Run clean up commands
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
            print ("\nUnexpected Error from command. Please check output: \n\n"+(out.decode('utf-8')))
            return
    
    archive_location = f"{s3_location}/venv_{dir_name}.tar.gz"
    print (f"Archive Location : {archive_location}\n")
    print (f'For Spark on YARN, use config "spark.yarn.dist.archives":"{archive_location}"')
    print (f'For Spark on Kubernetes, use config "spark.archives":"{archive_location}"')
           
