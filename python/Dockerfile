FROM python:3

RUN apt-get update

WORKDIR /home/app

# If we add the requirements and install dependencies first, docker can use cache if requirements don't change
ADD requirements.txt /home/app
RUN pip install --no-cache-dir -r requirements.txt

ADD . /home/app

# Run the command on container startup
CMD python3 bitable_calendar.py

