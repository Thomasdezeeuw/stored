# Coeus

Coeus is a distributed value store. Coeus is not a key-value store, as the key
isn't the decided by the user but by the sha512 sum of the stored value. It
supports three operations; storing values, reading values and removing values.
As the key of a value is its sha512 sum it not possible to modify values. If a
value needs to be modified a new value simply needs to stored and the new key
used.

The client can validate the correct delivery of the value by using the returned
key, which is the sha512 sum of the stored value.

TODO: describe use case(s).
TODO: values are blobs, unchanged by Coeus.

## Operations

Coeus supports three operations; storing values, reading values and removing
values.

TODO: remove commands and change it to describing the operations.

### Storing values

Storing values can be done with the `SET` command, it requires the `data` to be
stored and needs to know it's `size`.

The commands returns the sha512 hash of the `$data`.

#### Examples

```
> STORE "Hello world"
< 81381f1dacd4824a6c503fd07057763099c12b8309d0abcec4000c9060cbbfa67988b2ada669ab4837fcd3d4ea6e2b8db2b9da9197d5112fb369fd006da545de
> STORE "Hello World"
< e1c112ff908febc3b98b1693a6cd3564eaf8e5e6ca629d084d9f0eba99247cacdd72e369ff8941397c2807409ff66be64be908da17ad7b8a49a2a26c0e8086aa
```

### Reading values

Getting a value in one go, or streaming the contents.

TODO: not found.

```
GET $hash
```


#### Examples

```
> GET "81381f1dacd4824a6c503fd07057763099c12b8309d0abcec4000c9060cbbfa67988b2ada669ab4837fcd3d4ea6e2b8db2b9da9197d5112fb369fd006da545de"
< "Hello world"
> GET "e1c112ff908febc3b98b1693a6cd3564eaf8e5e6ca629d084d9f0eba99247cacdd72e369ff8941397c2807409ff66be64be908da17ad7b8a49a2a26c0e8086aa"
< "Hello World"
```

### Removing values

TODO: not found.

```
REMOVE $hash
```
