import subprocess
import os
import tempfile

def test_insert_and_select(repl_runner):
    output, _ = repl_runner([
        "insert 1 user1 user1@example.com",
        "select"
    ])
    assert "(1, user1, user1@example.com)" in output

def test_error_on_table_full(repl_runner):
    cmds = [f"insert {i} user{i} user{i}@example.com" for i in range(1, 1401)]
    cmds.append("insert 1401 overflow overflow@example.com")
    output, _ = repl_runner(cmds)
    assert "Error: Table Full." in output

def test_long_username_emal(repl_runner):
    long_username = "a" * 32
    long_email = "a" * 255
    cmds = [
        "insert 1 {long_username} {long_email}",
        "select"
    ]
    output, _ = repl_runner(cmds)
    assert "(1, {long_username}, {long_email})" in output 

def test_too_long_username_email(repl_runner):
    long_username = "a" * 33 
    long_email = "a" * 256 
    cmds = [
        f"insert 1 {long_username} {long_email}",
        "select"
    ]
    output, _ = repl_runner(cmds)
    assert "Error: String is too long" in output 

def test_negative_id(repl_runner):
    id = -1
    username = "user"
    email = "user@example.com"
    cmds = [
        f"insert {id} {username} {email}",
        "select"
    ]
    output, _ = repl_runner(cmds)
    assert "Error: Negative id" in output 

def test_persistence_to_disk(repl_runner):
    id = 1
    username = "user"
    email = "user@example.com"

    cmds1 = [
        f"insert {id} {username} {email}"
    ]
    output1, _ = repl_runner(cmds1)
    assert "Executed" in output1
    
    cmds2 = [
        "select"
    ]
    output2, _ = repl_runner(cmds2)
    assert f"({id}, {username}, {email})" in output2


