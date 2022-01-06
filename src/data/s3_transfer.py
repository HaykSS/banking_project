import boto3

s3 = boto3.client('s3')
# s3.upload_file('branches.txt','csvdocs','branches')

s3.upload_file('general_ledger.txt', 'csvdocs', 'general_ledger')