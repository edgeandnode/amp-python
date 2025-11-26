import json


class Abi:
    def __init__(self, path):
        f = open(path)
        data = json.load(f)
        self.events = {}
        for entry in data:
            if entry['type'] == 'event':
                self.events[entry['name']] = Event(entry)


# An event from a JSON ABI
class Event:
    def __init__(self, data):
        self.name = data['name']
        self.inputs = []
        self.names = []
        for input in data['inputs']:
            param = input['type']
            self.names.append(input['name'])
            if input['indexed']:
                param += ' indexed'
            param += ' ' + input['name']
            self.inputs.append(param)

    def signature(self):
        sig = self.name + '(' + ','.join(self.inputs) + ')'
        return sig
