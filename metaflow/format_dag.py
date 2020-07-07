import sys
import json
import tarfile
from subprocess import check_output

from metaflow import S3
from metaflow.graph import FlowGraph

def parse_dag(flow_name, script):
    with open(script) as f:
        graph = FlowGraph(source=f.read(), name=flow_name)
        nodes = {}
        for n in graph:
            nodes[n.name] = {
                'type': n.type,
                'box_next': n.type not in ('linear', 'join'),
                'box_ends': n.matching_join,
                'next': n.out_funcs
            }
    print(json.dumps(nodes))

def load_dag(flow_name, code_package_url):
    from tempfile import TemporaryDirectory

    s3 = S3()
    try:

        if code_package_url.startswith('s3://'):
            local = s3.get(code_package_url)
            fname = local.path
        else:
            fname = code_package_url

        tar = tarfile.open(fname)
        info = json.load(tar.extractfile('INFO'))
        with TemporaryDirectory(dir='.') as tempdir:
            tar.extract(info['script'], path=tempdir)
            py = 'python%s' % info['python_version_code'][0]
            cmd = [py, __file__, 'parse', flow_name, info['script']]
            out = check_output(cmd, cwd=tempdir)
            return json.loads(out.decode('utf-8'))

    finally:
        s3.close()

if __name__ == '__main__':
    if sys.argv[1] == 'format':
        print(json.dumps(load_dag(sys.argv[2], sys.argv[3]), indent=4))
    else:
        parse_dag(sys.argv[2], sys.argv[3])
