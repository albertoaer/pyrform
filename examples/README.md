# Pyrform examples

You can launch this directory typing `cd examples; pyrform .` or `pyrform ./examples` from the root. Doing so, all the workers would be available to perform requests.

## Hello World

### name: hello_world.py

This worker will return a *"hello world!"* string. It's just a dumb example to check if it's actually working.
```json
{
  "worker": "hello_world"
}
```

Also, you can override the default function. The function `debug` defined in this file will print the task to the terminal, useful to debug parameters.
```json
{
  "worker": "hello_world",
  "function": "debug"
}
```

## Small Cache DB

### name: db.py

This worker holds a shared dictionary used as a small db (note: don't update the source, since all cached items would be removed).
```json
{
  "worker": "db",
  "function": "add",
  "args": ["hello", "world"]
}
```

The outcume of this would be None. We can retrive the value using the following input:
```json
{
  "worker": "db",
  "function": "get",
  "args": ["hello"]
}
```

In order to remove it, we can update the previous request with the `remove` function. In order to remove after a while, we can include a delay in seconds (**500 ms in the following example**):
```json
{
  "worker": "db",
  "function": "remove",
  "args": ["hello"],
  "delay": 0.5
}
```