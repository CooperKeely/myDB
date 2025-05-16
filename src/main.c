#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>

// defines
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_PAGES 100

// macros
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

// result enums
typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED, 
}MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG,
    PREPARE_NEGATIVE_ID,
    PREPARE_UNRECOGNIZED_STATEMENT 
}PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT 
}StatementType;

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL,
    EXECUTE_FAILED
}ExecuteResult;

typedef enum{
    NODE_INTERNAL,
    NODE_LEAF
} NodeType;

// data structures
typedef struct {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
}InputBuffer;

typedef struct {
    uint32_t id; 
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1]; 
}Row;

typedef struct{
    StatementType type; 
    Row row_to_insert;
}Statement;

typedef struct{
    int file_descriptor;
    void* pages[TABLE_MAX_PAGES];
    uint32_t file_length;
    uint32_t num_pages;
}Pager;

typedef struct{
    Pager* pager;
    uint32_t num_rows;
    uint32_t root_page_num;
}Table;

typedef struct{
    Table* table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;
} Cursor;

// const defines for table
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_SIZE + USERNAME_OFFSET;
const uint32_t ROW_SIZE = EMAIL_SIZE + EMAIL_OFFSET; 

const uint32_t PAGE_SIZE = 4096;

// node header layout
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t  COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE; 

// leaf node header layout
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// leaf node body layout
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_OFFSET;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE; 
const uint32_t LEAF_NODE_SPACE_FOR_CELLS =  PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

// table functions
void print_row(Row* row){
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void serialize_row(Row* source, void* destination){
    memcpy(destination + ID_OFFSET, &source->id, ID_SIZE);
    strncpy(destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);
    strncpy(destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination){
    memcpy(&destination->id, source + ID_OFFSET, ID_SIZE);
    memcpy(&destination->username, source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&destination->email, source + EMAIL_OFFSET, EMAIL_SIZE);
}

void* get_page(Pager* pager, uint32_t page_num){
    if(page_num > TABLE_MAX_PAGES){
        printf("Error: Tried to fetch page out of bounds %d > %d \n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if(pager->pages[page_num] == NULL){
        // Cache Miss Allocate Memory and load from file
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;

        // save a partial page
        if(pager->file_length % PAGE_SIZE){
            num_pages += 1;
        }
        
        if(page_num <= num_pages){
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);

            if(bytes_read == -1){
                printf("Error: Reading file failed %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }
        
        pager->pages[page_num] = page;
        
        if(page_num >= pager->num_pages){
            pager->num_pages = page_num + 1;
        }
    }
    return pager->pages[page_num];
}

// leaf node functions
uint32_t* leaf_node_num_cells(void* node){
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void* leaf_node_cell(void* node, uint32_t cell_num){
    return node + LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_CELL_SIZE * cell_num;
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num){
    return leaf_node_cell(node,cell_num);
}

void* leaf_node_value(void* node, uint32_t cell_num){
    return leaf_node_cell(node, cell_num) + LEAF_NODE_CELL_SIZE;
}

void initialize_leaf_node(void* node) {
    *leaf_node_num_cells(node) = 0;
}

void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value){
    void* node = get_page(cursor->table->pager, cursor->page_num);

    uint32_t num_cells = *leaf_node_num_cells(node);
    if(num_cells >= LEAF_NODE_MAX_CELLS){
        // node full
        printf("Splitting a leaf node not implemented");
        exit(EXIT_FAILURE);
    } 

    if(cursor->cell_num < num_cells){
        // make room for new cell
        for(uint32_t i = num_cells; i > cursor->cell_num; i --){
            memcpy(
                leaf_node_cell(node, i),
                leaf_node_cell(node, i-1),
                LEAF_NODE_CELL_SIZE);
        }
    }

    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, cursor->cell_num)) = key;
    serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

// Cursor functions
void* cursor_value(Cursor* cursor){
    uint32_t page_num = cursor->page_num;
    void* page = get_page(cursor->table->pager, page_num); 
    return leaf_node_value(page, cursor->cell_num); 
}

Cursor* table_start(Table* table){
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = table->root_page_num;
    cursor->cell_num = 0;
    void* root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->end_of_table = (num_cells == 0);

    return cursor;
}

Cursor* table_end(Table* table){
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = table->root_page_num;
    cursor->end_of_table = true;

    void* root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->cell_num = num_cells;

    return cursor;
}

void cursor_advance(Cursor* cursor){
    uint32_t page_num = cursor->page_num;
    void* node = get_page(cursor->table->pager, page_num);

    cursor->cell_num += 1;
    if(cursor->cell_num >= (*leaf_node_num_cells(node))){
        cursor->end_of_table = true;
    }
}

// pager functions
Pager* pager_open(const char* filename){
    int fd = open(filename,
            O_RDWR | O_CREAT, // Rread/Write mode or Create file if dosen't exists
            S_IWUSR | S_IRUSR // User read/write permission
    );
    
    if(fd == -1){
        printf("Error: Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager* pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = (file_length / PAGE_SIZE);

    if(file_length % PAGE_SIZE != 0){
        printf("Db file is not a whole number of pages. Corrupt file\n");
        exit(EXIT_FAILURE);
    }

    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++){
        pager->pages[i] = NULL;
    }

    return pager;
}

void pager_flush(Pager* pager, uint32_t page_num){
    if (pager->pages[page_num] == NULL){
        printf("Error: Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }
    
    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

    if(offset == -1){
        printf("Error: Failed to seek %d\n", errno);
        exit(EXIT_FAILURE);
    }

   ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
    
    if(bytes_written == -1){
        printf("Error: Failed to write to file %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

void db_close(Table* table){
    Pager* pager = table->pager;

    for(uint32_t i = 0; i < pager->num_pages; i ++){
        if(pager->pages[i] == NULL){
            continue;
        }
        pager_flush(pager, i);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }
   
    int result = close(pager->file_descriptor);
    if(result == -1){
        printf("Error: Couldn't close DB file\n");
        exit(EXIT_FAILURE);
    }
    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i ++){
        void* page = pager->pages[i];
        if(page){
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
    free(table);
}

Table* db_open(const char* filename){
    Pager* pager = pager_open(filename);
    
    Table* table = (Table*) malloc(sizeof(Table));
    table->pager = pager;
    table->root_page_num = 0;
    if(pager->num_pages == 0){
        // new database file initialize page 0 as leaf node
        void* root_node = get_page(pager, 0);
        initialize_leaf_node(root_node);
    }
    return table;
}

// functions
InputBuffer* new_input_buffer(){
    InputBuffer* input_buffer = (InputBuffer*) malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;
    return input_buffer;
}

void print_prompt(){
    printf("myDB>");
};

void read_input(InputBuffer* input_buffer){
    errno = 0;
    ssize_t bytes_read = 
        getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);
    
    // check if getline failed
    assert(errno != EINVAL);
    assert(errno != ENOMEM);
    assert(!(bytes_read <= 0));
    
    input_buffer->input_length = bytes_read;
    if (input_buffer->buffer[bytes_read - 1] == '\n'){
        input_buffer->buffer[bytes_read - 1] = '\0';
        input_buffer->input_length -= 1;
    }
}

void close_input_buffer(InputBuffer* input_buffer){
    free(input_buffer->buffer);
    free(input_buffer);
}

// meta commands
void print_constants() {
  printf("ROW_SIZE: %d\n", ROW_SIZE);
  printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
  printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
  printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
  printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
  printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void print_leaf_node(void* node){
   uint32_t num_cells = *leaf_node_num_cells(node);
   printf("leaf (size %d)\n", num_cells);
   for(uint32_t i = 0; i < num_cells; i++){
       uint32_t key = *leaf_node_key(node, i);
       printf("  - %d : %d\n", i, key);
   }
}

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table){
    if(strcmp(input_buffer->buffer, ".exit") == 0){
        close_input_buffer(input_buffer);
        db_close(table); 
        exit(EXIT_SUCCESS);
    }else if(strcmp(input_buffer->buffer, ".constants") == 0){
        printf("Constants: \n");
        print_constants();
        return META_COMMAND_SUCCESS;
    }else if(strcmp(input_buffer->buffer, ".btree") == 0){
        printf("Tree: \n");
        print_leaf_node(get_page(table->pager, 0));
        return META_COMMAND_SUCCESS;
    }

    else{
        return META_COMMAND_UNRECOGNIZED;
    }
}

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement){
    statement->type = STATEMENT_INSERT;
    
    char* keyword = strtok(input_buffer->buffer, " ");
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");
    
    if(id_string == NULL || username == NULL || email == NULL){
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_string);
    if ( id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    if (strlen(username) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    if (strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement){
    if(strncmp(input_buffer->buffer, "insert", 6) == 0){
        return prepare_insert(input_buffer, statement);
    }
    if(strcmp(input_buffer->buffer, "select") == 0){
        statement->type = STATEMENT_SELECT; 
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

// execute functions
ExecuteResult execute_insert(Statement* statement, Table* table){
    void* node = get_page(table->pager, table->root_page_num);
    
    if((*leaf_node_num_cells(node) >= LEAF_NODE_MAX_CELLS)){
        return EXECUTE_TABLE_FULL;
    }

    Row* row_to_insert = &statement->row_to_insert;
    Cursor* cursor = table_end(table);

    leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

    free(cursor);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table){
    Cursor* cursor = table_start(table);
    Row row;
    
    while(!(cursor->end_of_table)){
        deserialize_row(cursor_value(cursor), &row);
        print_row(&row);
        cursor_advance(cursor);
    }

    free(cursor); 
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table){
    switch(statement->type){
        case(STATEMENT_INSERT):
            return execute_insert(statement, table);
        case(STATEMENT_SELECT):
            return execute_select(statement, table);
        default:
            return EXECUTE_FAILED;
    }
}

// main loop
int main(int argc, char* argv[]){
    
    if (argc < 2){
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }
    
    char* filename = argv[1];
    Table* table = db_open(filename);
    InputBuffer* input_buffer = new_input_buffer();
    
    while(true){
        print_prompt();
        read_input(input_buffer);
        
        if (input_buffer->buffer[0] == '.'){
            switch(do_meta_command(input_buffer, table)){
                case(META_COMMAND_SUCCESS):
                    continue;
                case(META_COMMAND_UNRECOGNIZED):
                    printf("Unrecognized Command: '%s' \n", input_buffer->buffer);
                    continue;
            }
        }
        
        Statement statement;
        switch(prepare_statement(input_buffer, &statement)){
            case(PREPARE_SUCCESS):
                break;
            case(PREPARE_SYNTAX_ERROR):
                printf("Syntax Error: Could not parse statement\n");
                continue;
            case(PREPARE_STRING_TOO_LONG):
                printf("Error: String is too long\n");
                continue;
            case(PREPARE_NEGATIVE_ID):
                printf("Error: Negative id\n");
                continue;
            case(PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized Keyword: at start of '%s' \n", input_buffer->buffer);
                continue;
        }
        switch(execute_statement(&statement, table)){
            case(EXECUTE_SUCCESS):
                printf("Executed\n");
                break;
            case(EXECUTE_TABLE_FULL):
                printf("Error: Table Full.\n");
                break;
            case(EXECUTE_FAILED):
                printf("Error: Unrecognized Execute Statement\n");
                break;
        }
    }
    exit(EXIT_SUCCESS);
}
