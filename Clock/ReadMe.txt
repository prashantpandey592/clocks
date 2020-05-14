The Following program can be executed in DataProc in GCP, (Whereas It will need authorization with) following requirment -:
1) Spark version 2.4 or above
2) Python 3
3) Need one script to up the cluster and delete the cluster once process is complete. 
4) To execute the code Use the following commad -:
spark-submit path_of_the_python_script path_of_the_input_file path_of_the_input_file.

While writting the code I have made following assumption -:
a) We have only one column in the file "Time" in the format 3:20 
b) I have excuted the code in my local system and at small dataset, whereas when it comes to larger data set we will need optamization.

Acceptance Criteria:-
1) How will you deploy this solution (in code or as a todo list if time is limited). i.e. how and where will this run?
We can deploy this code in multiple ways, The best practice will be as following -:
a) deploy code in the bucket along with other config file, 
b) Keep input data and output in bucket
c) Create a DAG and schedule the DAG in composer.

Apart from this we can use DataFlow or Big Query for ETL as per our requirment. 

2) How will you manage any infrastructure needed?
 As we deploying our code into GCP (DataProc), and Its a manages service, but still I will recommend to have script to control 
 resource allocation for example, max_executor etc. 
 
3) Any DevOps/Cicd components that would support this feature in a production setting
For CI/CD we can use deployment tools for example jenkins. 