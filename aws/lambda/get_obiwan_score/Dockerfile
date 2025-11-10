FROM public.ecr.aws/lambda/python:3.10

WORKDIR /var/task

RUN pip install uv
COPY get_obiwan_score/pyproject.toml .
COPY get_obiwan_score/uv.lock .
RUN uv export --frozen --no-emit-workspace --no-dev --no-editable -o requirements.txt
RUN pip install -r requirements.txt

COPY lib lib
COPY get_obiwan_score/lib/obi_wan lib/obi_wan

COPY get_obiwan_score/app.py .

CMD ["app.lambda_handler"]
