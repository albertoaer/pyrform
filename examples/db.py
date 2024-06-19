def worker(_):
  return "please use one of the DB actions: add(name, value), get(name), remove(name)"

db = {}

def add(task):
  name, value = task.args
  db[name] = value

def get(task):
  name, = task.args
  return db[name]

def remove(task):
  name, = task.args
  value = db[name]
  del db[name]
  return value