from SPARQLWrapper import SPARQLWrapper, JSON, XML, N3, RDF, CSV, TSV
import requests

class NeptuneWrapper:
    pass

# For Loading in the dataset
def load_in_data(connection_string,s3_source_file,format_of_file="rdfxml",iam_arn_role="arn:aws:iam::complete_role_which_gives_neptune_access_to_rds:role/rolename"):
    """

    :param connection_string: Instance Connection String from the Neptune with port : str
            example: 'NAME.uniqueresource.us-east-1.neptune.amazonaws.com'

    :param s3_source_file: source file location in s3 with extension: str
            example: 's3://bucketName/filename_with_extension'

    :param format_of_file: Format defination of the file(extension) : str
            example: 'rdfxml'

    :param iam_role: IAM policy with the access of s3 (listing and reading) : 'string'
        example:
            "arn:aws:iam::complete_role_which_gives_neptune_access_to_rds:role/rolename"

    :return:
    
    
    """
    # Do job submission here 
    headers = {'Content-Type': 'application/json'}
    
    data = '\n    {\n      "source" : "%s",\n      "format" : "%s" ,\n      "iamRoleArn" : "%s",\n      "region" : "us-east-1",\n      "failOnError" : "FALSE",\n      "parallelism" : "MEDIUM"\n      \n    }'%(s3_source_file,format_of_file,iam_arn_role)
    # print(data)
    response = requests.post('http://' + connection_string+':8182/loader',
    headers=headers, data=data, verify=False) # Response query with respect to the connection string ! 
    print(response.text)
    payload_job_id = response.text["payload"]["loadId"]
    return payload_job_id # returns the payload job id to be used for the later onward queries.


# A Job Status for the response 
def job_status_check(connection_string, job_id=''):
    """
        :param: connection_string
        :param: job_id : A job identifier from neptine : hexa string
    """
    # loading_staus = "IN_PROGRESS"
    response = requests.get(
        'http://'+connection_string+':8182/loader/'+job_id+'?details=true&errors=true&page=1&errorsPerPage=3').text # Recieves all the information of the specific job
    obj_json = json.loads(response) # json loading of the job identifier 
    loading_status = obj_json["payload"]["overallStatus"]["status"]
    print("Loading Status: ",loading_status)
    print("Overall Status Json Obj : ", obj_json["payload"]["overallStatus"])
    
    
# Query execution with respect to a specfic seperator ! 
def query_execution(connection_string, query = "" , sep = "|"):
    """
        To Execute a Query using Neptune as an endpoint use this function to retrieve the results based on a specific seperater 
        @Params:
            @Param: connection_string : An endpoint Snorkel Connection with [ENDPOINT] : string
            @Param: query: A Sparql query for the execution on endpoint, must be based on the data being provided : str (triple quoted)
            @Param: sep : A Seperator for the csv whether it has to be Pipe (|) or "," depends on requirements : str (default : "|")
        @Returns
            @return: 
    """
    endpoint = "http://"+connection_string + ":8182/sparql"
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    # XML example
    sparql.setReturnFormat(JSON) # Default Json to be the returning format ! 
    results = sparql.query().convert() # Query conversion to python readable format 
    for result in results["results"]["bindings"]: # json result based bindings search with respect to keys_for_consideration ! 
        keys_to_consider = list(result.keys()) # Gets all the key binding in the provided CSV 
        record = [result[key]['value'] for key in keys_to_consider] # Gets all the values with respect to the specific keys  
        print(sep.join(record)) # Prints the statement with respect to the delimeter specified ! 


# A Connection String 

connection_string = "CLUSTERENDPOINT_COPY_PASTE_FROM_AWS_ACCOUNT"

# Submitting a job : should Yield a job id

s3_source_file = "sourcefile"
load_in_data(connection_string=connection_string,s3_source_file = s3_source_file)



job_id = "##"
# JOB Status check after submitting 
# job_status_check(connection_string,job_id = job_id+)

query ="""WRITE YOUR SPARQL QUERY HERE """
# Running the query based on the provided file ! 
query_execution(connection_string = connection_string, query=query_for_complete_pipes_data)



