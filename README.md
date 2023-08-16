# pangeo-forge-se-tim
Demos of workflow for NASA's SE-TIM 2023 workshop.

## Instructions to Access the Demo
Go to: https://tinyurl.com/pangeo-forge-se-tim-register to submit your GitHub username if you haven't already done so to access the hub: https://tinyurl.com/pangeo-forge-se-tim-demo

## How to Run this Demo
1. Navigate to ```create-edl-credentials.ipynb``` to create a .netrc file with your Earth Data Login credentials. 
2. View or change the recipe to point to a collection of your choice under ```feedstock/zarr_recipy.py```. If using the example data collection ```GPM_3IMERGDL``` you will have to accept the NASA GES DISC EULA. 
3. View or change the provenance file ```feedstock/meta.yaml```.
4. In the JupyterHub, open a terminal and navigate to the directory 
```cd pangeo-forge-se-tim/```
5. In the terminal run pangeo-forge with the following command. The ```job_name``` should match the ``id`` provided in the ```meta.yaml``` file:
```pangeo-forge-runner bake --repo=. --Bake.job_name="gpm-zarr" --prune```

