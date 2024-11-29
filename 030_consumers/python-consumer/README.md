docker build -t weather-consumer .
docker run -d --name weather-consumer -v $(pwd)/data:/app/data --network="host" weather-consumer

docker run -d --name weather-consumer --network="host" weather-consumer

## Get Kafka IP
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' kafka