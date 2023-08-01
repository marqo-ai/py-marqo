name: py_marqo open source unit tests

on:
  workflow_dispatch:
    inputs:
      push_to:
        description: 'Docker registry location. Options: "ECR" or "DockerHub"'
        required: true
        default: 'DockerHub'
      image_repo:
        description: 'Marqo docker image repo name'
        required: true
        default: 'marqo'
      image_tag:
        description: 'Marqo image tag. Examples: "1.1.0", "test" "latest"'
        required: true
  push:
    branches: 
      - mainline
  pull_request:
    branches: 
      - mainline

permissions:
  contents: read

jobs:
  Start-Runner:
    name: Start self-hosted EC2 runner
    runs-on: ubuntu-latest
    outputs:
      label: ${{ steps.start-ec2-runner.outputs.label }}
      ec2-instance-id: ${{ steps.start-ec2-runner.outputs.ec2-instance-id }}
    steps:
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v1
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ secrets.AWS_REGION }}
      - name: Start EC2 runner
        id: start-ec2-runner
        uses: machulav/ec2-github-runner@v2
        with:
          mode: start
          github-token: ${{ secrets.GH_PERSONAL_ACCESS_TOKEN }}
          ec2-image-id:  ${{ secrets.AMD_EC2_IMAGE_ID }}
          ec2-instance-type: t3.xlarge
          subnet-id: ${{ secrets.AMD_SUBNET_ID }}
          security-group-id: ${{ secrets.AMD_SECURITY_GROUP_ID }}

  Test-Py-Marqo:
    name: Run Py-Marqo Test Suite
    needs: Start-Runner
    runs-on: ${{ needs.start-runner.outputs.label }}
    
    environment: py-marqo-test-suite

    steps:
      - name: Checkout py-marqo repo
        uses: actions/checkout@v3
        with:
          fetch-depth: 0

      - name: Set up Python 3.8
        uses: actions/setup-python@v3
        with:
          python-version: "3.8"
          cache: "pip"

      - name: Log into ECR
        uses: docker/login-action@v1
        if: github.event.inputs.push_to == 'ECR'
        with:
          registry: ${{ secrets.AWS_ACCOUNT_NUMBER }}.dkr.ecr.us-east-1.amazonaws.com/${{ github.event.inputs.image_repo }}
          username: ${{ secrets.AWS_ACCESS_KEY_ID }}
          password: ${{ secrets.AWS_SECRET_ACCESS_KEY }}

      - name: Set registry and image repo
        id: prepare
        run: |
          if [[ "${{ github.event.inputs.push_to }}" == "ECR" ]]; then
            echo "::set-output name=registry::${{ secrets.AWS_ACCOUNT_NUMBER }}.dkr.ecr.us-east-1.amazonaws.com"
          else
            echo "::set-output name=registry::marqoai"
          fi

      - name: Run Py-Marqo Tests
        run: |
          docker pull ${{ steps.prepare.outputs.registry }}/${{ github.event.inputs.image_repo }}:${{ github.event.inputs.image_tag }}
          docker run --name marqo -it --privileged -p 8882:8882 --add-host host.docker.internal:host-gateway \
            ${{ steps.prepare.outputs.registry }}/${{ github.event.inputs.image_repo }}:${{ github.event.inputs.image_tag }}
          python -m pip install --upgrade pip
          pip install tox
          tox

  Stop-Runner:
    name: Stop self-hosted EC2 runner
    needs:
      - Start-Runner
      - Test-Py-Marqo
    runs-on: ubuntu-latest
    if: ${{ always() }}
    steps:
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v1
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ secrets.AWS_REGION }}
      - name: Stop EC2 runner
        uses: machulav/ec2-github-runner@v2
        with:
          mode: stop
          github-token: ${{ secrets.GH_PERSONAL_ACCESS_TOKEN }}
          label: ${{ needs.start-runner.outputs.label }}
          ec2-instance-id: ${{ needs.start-runner.outputs.ec2-instance-id }}
