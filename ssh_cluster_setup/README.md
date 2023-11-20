## SSH Dask Cluster
In the case of setting up dask clusters not managed through integrated cloud provider solutions, we consider that there are typically 2 scenarios:  
1. Setting up the cluster with virtual machines on the cloud.  
2. Setting up the cluster with client's own machines.  
Generally speaking, steps for setting up the machines are similar for both cases. They can be separated into 2 steps:  
1. Install requirements on the machines.  
2. Setup the cluster through dask python interface.  
We will provide our python code in this folder. 
### More recommendations for setting up SSH cluster on the cloud
If our client choose to bring their own infrastructure, it is up to our client to configure their network suitable for SSH cluster setup.  
In contrast, we have conducted extensive research in the scenarion where our client chooses to setup the machines through a cloud provider. We experimented with EC2 clusters on AWS. Here are our findings:
* If we run the setup script from within the VPC (e.g. on one of the EC2 machines we've prepared), the cluster can be setup successfully.
* If we run the setup script from a machine from outside the VPC (e.g. our local machine), the setup script will run into a complicated IP binding problem. We ourselves have tried to resolve the problem, but failed after days of work. Many attempt have been made in [this stackoverflow post](https://stackoverflow.com/questions/74265724/best-practices-when-deploying-dask-cloudprovider-ec2-cluster-while-allowing-publ). Some looks promising to us, but we have decided that those are too complicated for the purpose of our tutorial. If the users **have to go through so many trouble to achieve the same thing they would have achieved simply by moving the script to within the VPC**, then it is pointless trying to educate our users on that subject matter.
* In conclusion, we recommend **simply run the script from within the VPC**. Of course, as Dask continues to develope, this may be resolved in the future.
* Our example script is also included in this folder.