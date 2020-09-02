import os
import json
import hashlib
from subprocess import Popen, PIPE

from metaflow.client.cache import CacheAction

class Search(CacheAction):

    @classmethod
    def format_request(cls, step_id, artifact_name, query):
        msg = {
            'step_id': step_id,
            'artifact_name': artifact_name,
            'query': query
        }
        search_expr = '/'.join((step_id, artifact_name, query))
        search_id = str(hashlib.sha1(search_expr.encode('utf-8')))
        result_key = 'search:result:%s' % search_id
        artifact_key = 'search:artifact:%s/%s' % (step_id, artifact_name)
        stream_key = 'search:stream:%s' % search_id

        return msg,\
               [result_key, artifact_key],\
               stream_key,\
               [stream_key, result_key]


    @classmethod
    def response(cls, keys_objs):
        return keys_objs

    @classmethod
    def stream_response(cls, it):
        for msg in it:
            if msg is None:
                yield msg
            else:
                yield {'event': msg}

    @classmethod
    def execute(cls,
                message=None,
                keys=None,
                stream_output=None,
                **kwargs):

        script = os.path.join(os.path.dirname(__file__), 'get_artifacts.py')
        cmd = ['python3',
               '-u',
               script,
               message['step_id'],
               message['artifact_name']]
        proc = Popen(cmd, stdout=PIPE, encoding='utf-8')

        for line in proc.stdout:
            stream_output({'progress': line.strip()})

        result_key = [k for k in keys if k.startswith('search:result:')][0]
        artifact_key = [k for k in keys if k.startswith('search:artifact:')][0]
        results = {}

        with open('artifacts.json') as f:
            results[artifact_key] = msg = f.read()
            values = json.loads(msg)

        query = message['query']
        matches = [task_id for task_id, [success, val] in values.items()\
                   if success and val == query]

        stream_output({'matches': matches})

        results[result_key] = json.dumps({'matches': matches})
        return results

