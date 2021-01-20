To run :

- Launch a Jupyter Docker Container:

`docker run -e GRANT_SUDO=yes --user root -p 8888:8888 jupyter/minimal-notebook:latest`

- Login to Jupyter using the link in the Jupyter logs
- Upload the 3 files in this repo
- Execute the cells in the EMR_on_EKS_Starter.ipynb notebook changing the content of variables where applicable.
e.g. change <bucket> in start-job-run-request.json or <eks-cluster-name> in the EMR_on_EKS_Starter.ipynb notebook.
