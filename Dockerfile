FROM public.ecr.aws/lambda/python:3.8.2023.02.03.11

ARG UNCACHE=2
COPY requirements.txt  .
RUN pip3 install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy function code
COPY app.py ${LAMBDA_TASK_ROOT}
CMD [ "predict_crop_type.handler" ]
