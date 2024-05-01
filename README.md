# ETL_Application
An ETL application which processes the provided customer, products and transaction datasets from the local file system and land the processed dataset back to a file system at a different location. 

## System Components
- **main.py**: This is the main Python script that creates the required tables on postgres (`customers`, `products`,`transactions`, `transactionlines`, `erasures` and `errorlog`).
- **database.py**: This is the Python script that contains the logic to connect to the Postgres instance, run the table creation and insert statements recieved from the main.py python script.

## Setting up the System
This Docker Compose file allows you to easily spin up Postgres application in Docker container. 

### Prerequisites
- Python 3.9 or above installed on your machine
- Docker Compose installed on your machine
- Docker installed on your machine


### Steps to Run
1. Clone this repository.
2. Navigate to the root containing the Docker Compose file.
3. Run the following command:

```bash
docker-compose up -d
```
This command will start Postgres container in detached mode (`-d` flag). Postgres will be accessible at `localhost:5432`.

### Running the App
1. Install the required Python packages using the following command:

```bash
pip install -r requirements.txt
```

2. Creating the required tables on Postgres and pulling the files from the `test-data` directory:

```bash
python main.py
```

### Future Development\ Extension

This could be extended further by having the data arrive onto a messaging queue e.g (Kafka topics, RabbitMQ) then consume from these topics and inserted into the database.

It appears it would be appropriate to have the transactional and customer data as a real-time stream, as these data events arrived throughout teh day. Whereas the product and erasure datasets could be implemented as batch ETL pipelines arriving once per day.