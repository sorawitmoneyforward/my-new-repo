FROM public.ecr.aws/lambda/python:3.10

WORKDIR /var/task

RUN pip install uv
COPY get_luke_score/pyproject.toml .
COPY get_luke_score/uv.lock .
RUN uv export --frozen --no-emit-workspace --no-dev --no-editable -o requirements.txt
RUN pip install -r requirements.txt

COPY lib lib

COPY get_luke_score/app.py .

CMD ["app.lambda_handler"]
