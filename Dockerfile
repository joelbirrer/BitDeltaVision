FROM python:3.9-alpine
COPY . /BitDeltaVision
WORKDIR /BitDeltaVision
RUN pip install -r requirements.txt # https://github.com/bndr/pipreqs
CMD ["python", "BitDeltaVision.py"] 
