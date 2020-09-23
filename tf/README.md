 To apply this configuration:

 1. ```export TF_ENV=[dev|stage|prod]```
 2. ```terraform init -backend-config=${TF_ENV}.conf```
 3. ```terraform workspace select crepo```
 4. ```terraform apply -var-file=${TF_ENV}.tfvars```
