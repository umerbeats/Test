#include <stdlib.h>
#include "linked_list.h"

// Creates and returns a new list
list_t* list_create()
{
    /* Malloc the list */
    list_t *list = (list_t *)malloc(sizeof(list_t));
    
    /* Set default values */
    if(list){
        list->head = NULL;
        list->tail = NULL;
        list->count = 0;
    }

    return list;
}

// Destroys a list
void list_destroy(list_t* list)
{
    /* Loop and free every node to avoid memory leaks */
    list_node_t* current_node = list->head;
    while (current_node) {
        list_node_t* next = current_node->next;
        free(current_node);
        current_node = next;
    }
    free(list);
}

// Returns head of the list
list_node_t* list_head(list_t* list)
{
    return list->head;
    return NULL;
}

// Returns tail of the list
list_node_t* list_tail(list_t* list)
{
    return list->tail;
    return NULL;
}

// Returns next element in the list
list_node_t* list_next(list_node_t* node)
{
    return node->next;
    return NULL;
}

// Returns prev element in the list
list_node_t* list_prev(list_node_t* node)
{
    return node->prev;
    return NULL;
}

// Returns end of the list marker
list_node_t* list_end(list_t* list)
{
    /* IMPLEMENT THIS IF YOU WANT TO USE LINKED LISTS */
    return NULL;
}

// Returns data in the given list node
void* list_data(list_node_t* node)
{
    return node->data;
}

// Returns the number of elements in the list
size_t list_count(list_t* list)
{
    return list->count;
}

// Finds the first node in the list with the given data
// Returns NULL if data could not be found
list_node_t* list_find(list_t* list, void* data)
{
    /* IMPLEMENT THIS IF YOU WANT TO USE LINKED LISTS */
    return NULL;
}

// Inserts a new node in the list with the given data
// Returns new node inserted
list_node_t* list_insert(list_t* list, void* data)
{
    /* Create a new node to insert */
    list_node_t *new_node = (list_node_t*)malloc(sizeof(list_node_t));

    /* Initalize node */
    if(new_node){
        new_node -> data = data;
        new_node -> next = NULL; // New nodes will always be at the tail 
        
        /* Check if list is empty */
        if(list -> head == NULL){
            list -> head = new_node;
            list -> tail = new_node;
            new_node -> prev = NULL;
        } else {
            new_node->prev = list->tail;
            list->tail->next = new_node;
            list->tail = new_node;
        }

        list->count++;
    }

    return new_node;
}

// Removes a node from the list and frees the node resources
void list_remove(list_t* list, list_node_t* node)
{
    /* Check if node to remove is head */
    if(node->prev){
        node->prev->next = node->next;
    } else {
        list->head = node->next;
    }

    /* Check if node to remove is tail */
    if(node->next){
        node->next->prev = node->prev;
    } else {
        list->tail = node->prev;
    }

    free(node);
    list->count--;
}
