FROM prefecthq/prefect:2.10.4-python3.10

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY parameterized_flow.py /opt/prefect/flows/parameterized_flow.py