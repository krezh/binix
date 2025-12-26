# Upload docker image to registry with test tag
upload-test:
    #!/usr/bin/env bash
    set -euo pipefail

    # Generate random number
    RANDOM_NUM=$RANDOM
    TAG="test-$RANDOM_NUM"

    # Get GitHub username
    GH_USER=$(gh api user --jq .login)
    IMAGE_NAME="ghcr.io/$GH_USER/binixd:$TAG"

    echo "Authenticating with GitHub Container Registry..."
    echo "$(gh auth token)" | docker login ghcr.io -u "$GH_USER" --password-stdin

    echo "Building docker image..."
    nix build .#docker-image

    echo "Loading docker image..."
    LOADED_IMAGE=$(docker load < result | grep -oP '(?<=Loaded image: ).*')
    echo "Loaded: $LOADED_IMAGE"

    echo "Tagging as $IMAGE_NAME..."
    docker tag "$LOADED_IMAGE" "$IMAGE_NAME"

    echo "Pushing $IMAGE_NAME..."
    docker push "$IMAGE_NAME"

    echo "Successfully pushed: $IMAGE_NAME"
