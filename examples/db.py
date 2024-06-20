def worker(_):
  return "please use one of the DB actions: add(name, value), get(name), remove(name)"

db = {}

def add(task):
  name, value = task.args
  db[name] = value

def add_with_lifetime(task):
  name, value, lifetime = task.args
  task.args = [name]
  task.function = "remove"
  task.delay = int(lifetime)
  task.queue_again()
  db[name] = value

def get(task):
  name, = task.args
  if name not in db: # prevent exception that would clean the module
    return None
  return db[name]

def remove(task):
  name, = task.args
  if name not in db: # prevent exception that would clean the module
    return None
  value = db[name]
  del db[name]
  return value