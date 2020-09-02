import sys
import json
import pickle
from gzip import GzipFile

from multiprocessing import Pool
from itertools import islice

sys.path.insert(0, '/data/metaflow')
from metaflow import namespace, Run, Step, Task, DataArtifact, S3

NUM_PROC = 32
METADATA_BATCH_SIZE = 128
S3_BATCH_SIZE = 512
MAX_SIZE = 4096

def batchiter(it, batch_size):
    it = iter(it)
    while True:
        batch = list(islice(it, batch_size))
        if batch:
            yield batch
        else:
            break

def get_s3url(artifact_id):
    namespace(None)
    return DataArtifact(artifact_id)._object['s3_location']

def decode(path):
    with GzipFile(path) as f:
        obj = pickle.load(f)
        return str(obj)

def start(step_id, artifact):
    namespace(None)
    artifacts = ['/'.join(task.pathspec.split('/')[1:] + [artifact])
                 for task in Step(step_id)]

    print("%d tasks found" % len(artifacts))

    s3urls = []
    num_metadata_batches = len(artifacts) // METADATA_BATCH_SIZE
    with Pool(processes=NUM_PROC) as pool:
        for i, arts in enumerate(batchiter(artifacts, METADATA_BATCH_SIZE)):
            s3urls.extend(zip(arts, pool.map(get_s3url, arts)))
            print('retrieving metadata %d/%d' % (i, num_metadata_batches))

    uniques = list(frozenset(s3url for _, s3url in s3urls))
    print("%d unique artifacts found" % len(uniques))
    num_s3_batches = len(uniques) // S3_BATCH_SIZE
    field_values = {}
    with S3() as s3:
        for i, urls in enumerate(batchiter(uniques, S3_BATCH_SIZE)):
            for s3obj in s3.get_many(urls):
                if s3obj.size < MAX_SIZE:
                    try:
                        field_values[s3obj.url] = [True, decode(s3obj.path)]
                    except Exception as ex:
                        field_values[s3obj.url] = [False, 'failed: %s' % ex]
                else:
                    field_values[s3obj.url] = [False, 'too large']

            print('retrieving S3 artifacts %d/%d' % (i, num_s3_batches))

    res = {'/'.join(art.split('/')[:-1]): field_values[s3url]
           for art, s3url in s3urls}

    with open('artifacts.json', 'w') as f:
        json.dump(res, f)

    print('artifacts retrieved')

if __name__ == '__main__':
    start(sys.argv[1], sys.argv[2])
