#!/usr/bin/env python3
"""Conservative AST import inventory. Unreferenced does not imply safe to delete."""
import ast
import json
from pathlib import Path


def audit(root):
    files=list((root/'src').rglob('*.py'))
    imports={}
    for path in files:
        tree=ast.parse(path.read_text())
        refs=[]
        for node in ast.walk(tree):
            if isinstance(node,ast.Import): refs.extend(x.name for x in node.names)
            elif isinstance(node,ast.ImportFrom): refs.append('.'*node.level+(node.module or ''))
        imports[str(path.relative_to(root))]=sorted(set(refs))
    return {'files':len(files),'imports':imports,'deletion_authorized':False,
            'limitations':'dynamic imports, executable scripts, historical replay and operational references require manual audit'}

if __name__=='__main__': print(json.dumps(audit(Path(__file__).resolve().parents[1]),indent=2))
