# base Docker image that we will build on
FROM prefecthq/prefect:2.7.7-python3.9

#  Copy the list of env packages needed. Don't forget what's on the list 
COPY docker_env_req.txt .

# Setup the env packages requirement 
RUN pip install -r docker_env_req.txt --trusted-host pypi.python.org --no-cache-dir
RUN prefect agent start -q default --api 'https://api.prefect.cloud/api/accounts/975bd9ed-5aef-4c8a-a413-23073fef3acb/workspaces/a711abba-f5ae-4b00-9315-34f92f089b77'


## https://datatalks-club.slack.com/archives/C01FABYF2RG/p1674907027290529

## run this to build the image
# docker image build -t reneboygarcia/prefect-docker-cloud .
# docker image build -t [docker_username/local_image:tag] .
# tag is optional

## Push the the image
# docker image push reneboygarcia/prefect-docker-cloud
# docker image push [docker_username/local_image:tag]

## You should see the pushed image in the remote repository in
## docker desktop

# prefect deployment run "ETL Parent/docker-flow"
