#! /bin/bash

RootPath=$(cd $(dirname $0)/..; pwd)

help() {
    cat <<EOF

Usage: ./run_docker.sh [ --build | --help ]
    --help              show help info
    --build             build ChubaoFS server and client
    --clean             clean up containers
    --clear             clear old docker image
EOF
    exit 0
}

# build
build() {
    mkdir -p ~/.cargo/registry
    mkdir -p ${RootPath}/docker/build
    docker-compose -f ${RootPath}/docker/docker-compose.yml run build  
}

# clean
clean() {
    docker-compose -f ${RootPath}/docker/docker-compose.yml down
}

cmd="help"

ARGS=( "$@" )
for opt in ${ARGS[*]} ; do
    case "$opt" in
        --help)
            help
            ;;
        --build)
            cmd=build
            ;;
        --clean)
            cmd=clean
            ;;
        *)
            ;;
    esac
done

case "-$cmd" in
    -help) help ;;
    -build) build ;;
    -clean) clean ;;
    *) help ;;
esac

