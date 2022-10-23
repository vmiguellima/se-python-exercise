# se-test
Steel eye test excercise

# Create custom python layer

```bash
aws lambda publish-layer-version --layer-name steel-eye-layer --zip-file fileb://layer.zip --compatible-runtimes python3.9 --region us-east-2
```

## Usefull links

- [AWS Lambda Python](https://docs.aws.amazon.com/lambda/latest/dg/lambda-python.html)
- [AWS Lambda Python Unit Testing](https://emshea.com/post/writing-python-unit-tests-lambda-functions)
- [AWS Lambda Python Unit Testing Example Github](https://github.com/em-shea/lambda-python-unit-test-example)
- [AWS Toolkit VS Code](https://docs.aws.amazon.com/toolkit-for-vscode/latest/userguide/setup-toolkit.html)
- [AWS Lambda basic example](https://docs.aws.amazon.com/code-samples/latest/catalog/python-lambda-lambda_handler_calculator.py.html)
- [AWS SAM MacOs](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install-mac.html)
- [SAM Config](https://github.com/aws/aws-sam-cli/blob/develop/designs/sam-config.md)
- [Testing Python](https://docs.pytest.org/en/7.1.x/how-to/assert.html)
- [Python pandas](https://pandas.pydata.org/docs/reference/api/pandas.read_xml.html)
- [Pandas Column to List](https://stackoverflow.com/questions/22341271/get-list-from-pandas-dataframe-column-or-row)