ARTIFACT_BUCKET=$(cat bucket-artifact.txt)

aws cloudformation package --template-file template.yml --s3-bucket $ARTIFACT_BUCKET --s3-prefix builds --output-template-file cfstack_pkgd.yml --profile default
aws cloudformation deploy --template-file cfstack_pkgd.yml --s3-bucket $ARTIFACT_BUCKET --s3-prefix builds --stack-name eedb015-2024-grupo-a --capabilities CAPABILITY_IAM --region us-east-1 --profile default