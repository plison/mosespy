echo "AHAHAHAHAAHAHAHAHAAHA================="
echo "SDFDFSFSADFSFS"
echo "$@"
mpirun "$(dirname "$0")"/../moses/bin/moses "$@"