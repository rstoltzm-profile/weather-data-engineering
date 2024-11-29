# Steps
1. create an account at openweathermap.org, get an api key for data
2. Make sure to rename config_template.yaml to config.yaml and input an api key.

## Start container
```bash
docker build -t weather-collector .
docker run -d --name weather-collector --network="host" weather-collector
```

## Rebuild image
```bash
docker stop weather-collector
docker rm weather-collector 
docker build -t weather-collector .
docker run -d --name weather-collector --network="host" weather-collector
```

## Get kafka ip
```bash
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' kafka
```