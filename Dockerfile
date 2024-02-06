# python3.11 lambda base image. 
FROM public.ecr.aws/lambda/python:3.11

# copy requirements.txt to container root directory
COPY requirements.txt ./

# installing dependencies from the requirements under the root directory
RUN pip install -r ./requirements.txt

# Copy function code to container
COPY main_parquet.py ./

# setting the CMD to your handler file_name.function_name
CMD [ "lambda_function.lambda_handler" ]