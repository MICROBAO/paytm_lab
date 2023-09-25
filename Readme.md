Please find the Pyhon code Weather_Analysis_Single_Machine.ipynb

The notebook with intermediate results can be found here Weather_Analysis_Single_Machine.md

The solution implemented right now is for running on laptop. In order to scale to larger dataset the following configs and design can be applied

- Assuming the source data is partitioned by year, we can partition weather data by adding a partition on year or year, month and day so the processing time can be reduced if only looking for specific range
- We can build data lake on s3 or other cloud services which can support transational operations and build responding Catalog for example if using AWS as the cloud provider, we can build Apache Iceberg tables by enable the iceberg config when creating the EMR cluster and use the glue catalog as the default catalog so that data scientist can also query the data by using Athena which is a presto supported SQL query interface
- We could write the results into s3 and build dashboards such as Looker or Tableau for BD team to query the results
- In the case that raw data is pretty big that batch processing may be too time consuming, we can load the raw data into the lake with Kappa architecture which loads data incrementally with the help of data lake features mentioned above.  
