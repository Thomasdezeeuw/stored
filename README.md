# Coeus

Coeus is a distributed immutable value store. Coeus is not a key-value store, as
the key isn't the decided by the user but by the checksum of the stored value.

It supports three operations: storing, retrieving and removing values. As the
key of a value is its checksum it not possible to modify values. If a value
needs to be modified a new value simply needs to stored and the new key used.
The client can validate the correct delivery of the value by using the returned
key. They values themselves are unchanged by Coeus and are seen as binary blobs.


## Operations

Coeus supports three operations; storing values, reading values and removing
values. But first we need to start the server.


### Starting the server

Starting the server is simple, just call `stored`.

```bash
$ stored
```

Next we using the various binaries to access the stored values.


### Storing a value

Storing values can be done with the `store` command. The commands returns the
key of value.


```bash
# Using the first argument
$ store "Hello world"
# Without arguments it reads from standard in.
$ echo "Hello world" | store
```


### Retrieving a value

Getting a value can be done using the `retrieve` command.

```bash
$ retrieve $key
```


### Removing a value

Removing a value can be done with `remove` command.

```bash
$ remove $key
```
