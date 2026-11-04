#!/usr/bin/env python3
"""Mount an external pinned protocol source bundle only while regenerating fixtures."""
import argparse,hashlib,json,os,pathlib,signal,subprocess
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--source',default=os.environ.get('BARTERLEND_PROTOCOL_FIXTURES'))
parser.add_argument('command',nargs=argparse.REMAINDER)
args=parser.parse_args()
if not args.source or not args.command:parser.error('external --source and command are required')
root=pathlib.Path(__file__).resolve().parents[1];source=pathlib.Path(args.source).resolve(strict=True)
if source==root or root in source.parents:parser.error('source bundle must be outside the repository')
fixture=root/'tests/fixtures/protocol-v5';mount=fixture/'solidity'
links=[(mount/name,source/name)for name in ['src','test','foundry.toml']]
for target,origin in links:
    if not origin.exists() or target.exists() or target.is_symlink():raise SystemExit('missing source or occupied mount: '+str(target))
attached=[];process=None
signal.signal(signal.SIGTERM,lambda *a:(_ for _ in ()).throw(KeyboardInterrupt()))
try:
    for target,origin in links:
        target.symlink_to(origin,target_is_directory=origin.is_dir());attached.append((target,origin))
    for entry in json.loads((fixture/'INPUTS.json').read_text())['files']:
        if hashlib.sha256((mount/entry['path']).read_bytes()).hexdigest()!=entry['sha256']:
            raise SystemExit('pinned fixture input changed: '+entry['path'])
    command=args.command[1:]if args.command[0]=='--'else args.command
    process=subprocess.Popen(command,cwd=root,start_new_session=True)
    raise SystemExit(process.wait())
finally:
    if process is not None and process.poll() is None:
        os.killpg(process.pid,signal.SIGTERM)
        try:process.wait(timeout=5)
        except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait()
    for target,origin in reversed(attached):
        if target.is_symlink()and target.resolve()==origin:target.unlink()
