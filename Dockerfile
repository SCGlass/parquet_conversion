# python3.11 lambda base image. 
FROM public.ecr.aws/lambda/python:3.11

# Set working directory
WORKDIR /app

# copy requirements.txt to container root directory
COPY requirements.txt ./

COPY src/.env ./

# installing dependencies from the requirements under the root directory
RUN pip install -r requirements.txt

# Copy function script to container
COPY src/main_parquet.py ./

# setting the CMD to your handler file_name.function_name
CMD ["main_parquet.process_lambda" ]