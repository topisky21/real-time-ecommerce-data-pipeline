FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

WORKDIR /template

COPY pipeline.py .
COPY requirements.txt .

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/pipeline.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"

RUN pip install --no-cache-dir -r requirements.txt