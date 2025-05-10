# https://pythonspeed.com/articles/activate-virtualenv-dockerfile/

FROM python:3.12-slim

WORKDIR /app

# Install dependencies:
COPY requirements.txt .
RUN pip install -r requirements.txt

# Run the application:
COPY . .
CMD ["./bot.py"]