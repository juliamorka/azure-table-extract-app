#!/usr/bin/env bash
set -e

terraform init
terraform validate
terraform plan -out=tfplan
echo
read -p "Apply this Terraform plan? (y/n): " confirm

if [[ "$confirm" != "y" ]]; then
  echo "Aborted."
  exit 0
fi

terraform apply tfplan
