# Store*d*


Store*d* (pronounced store-daemon, or just stored) is a distributed immutable
blob store. Stored is not a key-value store, as the key isn't the decided by the
user but by the SHA-512 checksum of the stored blob.

It supports three operations: storing, retrieving and removing blobs. As the key
of a blob is its checksum it is not possible to modify blobs. If a blob needs to
be modified a new blob simply needs to be stored and the new key used. The
client can validate the correct delivery of the blob by using the returned key
(checksum). The blob themselves are unchanged by Stored.


## Operations

Stored supports three operations; storing values, retrieving values and removing
values. But first we need to start the server.


### Starting the server

Starting the server is simple, just call `stored` with a path the configuration
file. See [`config.example.toml`] for all configuration options.

[`config.example.toml`]: ./config.example.toml

```bash
$ stored config.toml
```

Next we look at how to interact with stored.


### Storing a value

Storing values can be done with the `store` command. The commands returns the
key of value.


```bash
# Using the first argument
$ store "Hello world"
# Without arguments it reads from standard in.
$ echo "Hello world" | store
```

But since stored exposes an HTTP interface we can also use a tool like `curl` to
store values using a POST request.

```bash
# Store "Hello world".
curl -X POST -d "Hello world" -v http://127.0.0.1:8080/blob
# Store the contents of some_file.txt.
curl -X POST -d "@some_file.txt" -v http://127.0.0.1:8080/blob
```


### Retrieving a value

Getting a value can be done using the `retrieve` command.

```bash
$ retrieve $key
```

Or using the HTTP interface using `curl`.

```bash
# Retrieves "Hello world".
curl http://127.0.0.1:8080/blob/b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47
```


### Removing a value

Removing a value can be done with `remove` command.

```bash
$ remove $key
```

Or a simple DELETE request.

```bash
# Deletes "Hello world".
curl -X DELETE http://127.0.0.1:8080/blob/b7f783baed8297f0db917462184ff4f08e69c2d5e5f79a942600f9725f58ce1f29c18139bf80b06c0fff2bdd34738452ecf40c488c22a7e3d80cdf6f9c1c0d47
```
