@echo off


if %1 == --build  docker-compose -f docker-compose.yml run build


echo= Usage: ./run_docker.bat  --build
echo= --help              show help info
echo= --build             build ChubaoFS server and client
echo= --clean             clean up containers
echo= --clear             clear old docker image
