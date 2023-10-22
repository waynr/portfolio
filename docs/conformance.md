# Running OCI Conformance Tests

1. Make sure the distribution spec repo submodule is updated:
```shell
git submodule update
```

2. Run the tests:
```shell
just conformance-all
```

If tests are successful you should see terminal output that looks something
like:
```
Ran 74 of 79 Specs in 8.095 seconds
SUCCESS! -- 74 Passed | 0 Failed | 0 Pending | 5 Skipped
PASS
```

Some test cases may be skipped because many of the setup and teardown for
different test cases are implemented as test cases in the conformance test
suites test framework and may be disabled based on configuration parameters
hardcoded in the Justfile that defines the `conformance-all` command.
