FROM bytewax/bytewax as build

COPY requirements.txt .

RUN /venv/bin/pip install -r requirements.txt

FROM build

COPY dataflow.py .
