subscriptions = {}

def subscribe(token, handler):
    subscriptions.setdefault(token, []).append(handler)

def publish(token, data):
    for handler in subscriptions.get(token, []):
        handler(token, data)