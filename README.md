# portfolio

Portfolio is a modular implementation of the [distribution
spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md).
"Distribution" is a more obscure name for what is commonly understood as a
"container registry API", but can be used to store more than just container
images.

The primary focus in this implementation is on safe concurrent object storage
management via transactional metadata, but the project is designed in a way
that allows for a variety of backend implementations. For more info, please see
[architecture.md](./docs/architecture.md).

# License

This project is currently not licensed for general use and is not currently
seeking contributors. I eventually intend to add an open source license once
the project is in a more generally shareable state.
