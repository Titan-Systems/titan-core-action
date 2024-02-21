FROM python:3.11

COPY requirements.txt /requirements.txt

RUN python -m pip install --no-cache -r requirements.txt

COPY main.py /main.py

CMD ["python", "/main.py"]