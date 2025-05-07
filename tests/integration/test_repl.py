import subprocess
import os
import tempfile


def run_repl(commands):
    process = subprocess.Popen(
            ['./build/myDB'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
    )
    joined_commands = '\n'.join(commands + ['.exit\n'])
    stdout, stderr = process.communicate(joined_commands)
    return stdout, stderr

def test_insert_and_select():
    output, _ = run_repl([
        "insert 1 user1 user1@example.com",
        "select"
    ])
    assert "(1, user1, user1@example.com)" in output

def test_error_on_table_full():
    cmds = [f"insert {i} user{i} user{i}@example.com" for i in range(1, 1401)]
    cmds.append("insert 1401 overflow overflow@example.com")
    output, _ = run_repl(cmds)
    assert "Error: Table Full." in output
