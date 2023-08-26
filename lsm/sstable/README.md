## SS table

File layout

```
## Data Section

List of entries. Each entry is follows
Entry: 8 bytes + key len + value len
  key_len(int32)
  key
  is_deleted + value_len: (int32)
  value
  

## Sparse index

* key -> length, offset
* disk layout: Sequence of Entries. Each entry is as follows
  Entry: 12 bytes + key len
    key_len(int32)
    key_len bytes of key
    datasection_offset(int64)

## Footer section
* int64: number of bytes used for data
* int64: number of bytes used for sparse index
```