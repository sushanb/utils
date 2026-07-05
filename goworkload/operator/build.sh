# Initialize module if you haven't
go mod init my-operator
go mod tidy

# Build
export OP_IMAGE="us-central1-docker.pkg.dev/sbhattarai-test101/bigtable-utils/crd-operator:v3"
docker build --platform linux/amd64 -t $OP_IMAGE .
docker push $OP_IMAGE
