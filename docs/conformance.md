# Running OCI Conformance Tests

https://github.com/opencontainers/distribution-spec/tree/main/conformance

1. Prepare [localdev](../README.md) 

2. Checkout OCI conformance tests above

Note: all commands should be run from the conformance tests project

```shell
# Setup conformance tests
go test -c 
```

3. Set local environment
```shell
export OCI_ROOT_URL="http://localhost:13030/"
# Hardcoded 'meow' registry, repository name shouldn't matter
export OCI_NAMESPACE="meow/woof"

# TODO implement crossmount support
export OCI_AUTOMATIC_CROSSMOUNT=0
#export OCI_CROSSMOUNT_NAMESPACE=

# TODO implement authentication
export OCI_USERNAME="myuser"
export OCI_PASSWORD="mypass"

# TODO enable discovery / management 
export OCI_TEST_PULL=1
export OCI_TEST_PUSH=1
export OCI_TEST_CONTENT_DISCOVERY=0
export OCI_TEST_CONTENT_MANAGEMENT=0

# Extras
export OCI_HIDE_SKIPPED_WORKFLOWS=1
export OCI_DEBUG=1
export OCI_DELETE_MANIFEST_BEFORE_BLOBS=0
```

4. Run tests
```shell
./conformance.test
```

5. View results

Instead of scrolling through the test output, check out `report.html`
