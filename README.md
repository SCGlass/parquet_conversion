# CSV to Parquet pipeline using Amazon Web services.

This is a project to create a pipeline using a Lambda process within Amazon Web services (AWS) that will take a csv file from an S3 bucket apply a cleaning process to the data and then save as parquet format to another S3 bucket.

## The Process

I begin the project by creating a dummy dataset that had noticeable errors within the data. This was good practice to create dummy data and to be able to create bad data that would be cleaned and converted within the data processing stage. The development of my dummy data frame can be found in the ```dummy_data/dummy_data.ipynb``` jupyter notebook.

Here is an example of the Dummy data before processing:
<div align="center">
    <img src="assets/dummy_data_raw.png" width="600px"</img> 
</div>

I will briefly explain what the columns mean and what kind of bad data I included within them: 

- Timestamp: The timestamp is a Unix timestamp (https://www.unixtimestamp.com/). This is the format that the vessel registers time information. It is ten digits long with every entry. I made sure to include such errors as digits that were less than or more than ten digits, NaN values and string values.  

- Speed over ground: This column had a range between 0 to 10 for its standard. I included numbers that were out of the range. For example, minus numbers and numbers over 10. I also included NaN values and string values.  

- Longitude and Latitude: The same process was applied to these columns as with the speed over ground column. The difference was that the ranges were not the same. Longitude has a range between –180 and 180.  Latitude has a range between –90 and 90. I ensured that I added numbers out of these ranges. Nan values and string values were also applied. 

- Engine fuel rate: Again, this column had the same process as speed over ground, Longitude and Latitude columns. The range was between 0 and 100. Numbers were added out of this range in minus and positive form. NaN values and string values were also applied. 