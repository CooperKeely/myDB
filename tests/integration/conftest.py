import os
import tempfile
import subprocess
import pytest


@pytest.fixture
def repl_runner():
    #create a temp file for db
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        db_path = tmp.name

    def run(commands):
        process = subprocess.Popen(
                ['./build/myDB', db_path],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
        )
        joined_commands = '\n'.join(commands + ['.exit\n'])
        stdout, stderr = process.communicate(joined_commands)
        return stdout, stderr
    yield run

    os.remove(db_path)
