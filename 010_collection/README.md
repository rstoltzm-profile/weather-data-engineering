53673993860bf1ee8348478a50d2cdf2

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