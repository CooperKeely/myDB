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

def test_contants(repl_runner):
    cmds = [".constants"]
    output1, _ = repl_runner(cmds);
    assert "ROW_SIZE: 293" in output1
    assert "COMMON_NODE_HEADER_SIZE: 6" in output1
    assert "LEAF_NODE_HEADER_SIZE: 10" in output1
    assert "LEAF_NODE_CELL_SIZE: 297" in output1
    assert "LEAF_NODE_SPACE_FOR_CELLS: 4086" in output1
    assert "LEAF_NODE_MAX_CELLS: 13" in output1
    
def test_print_structure(repl_runner):
    cmds = [f"insert {i} user{i} person{i}@example.com" for i in [3, 1, 2]]
    cmds.append(".btree")
    output1, _ = repl_runner(cmds); 

    assert "Tree:" in output1
    assert "leaf (size 3)" in output1
    assert "  - 0 : 3" in output1
    assert "  - 1 : 1" in output1
    assert "  - 2 : 2" in output1




