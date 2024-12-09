#include "channel.h"

void notify_semaphores(list_t *list){
    /* Store each node */
    list_node_t *node = list->head;

    /* Go through each node and call post */
    while(node){
        sem_t *semaphore = (sem_t *) node->data;
        sem_post(semaphore);
        node = node->next;
    }
}

void insert_list_semaphore(select_t* channel_list, size_t channel_count, sem_t* semaphore, list_node_t **thread_nodes){
    for(size_t i = 0; i < channel_count; i++){
        pthread_mutex_lock(&channel_list[i].channel->mutex);
        thread_nodes[i] = list_insert(channel_list[i].channel->semaphore_list, (void*)semaphore);
        pthread_mutex_unlock(&channel_list[i].channel->mutex);
    }
}

void remove_list_semaphore(select_t* channel_list, size_t channel_count, list_node_t **thread_nodes){
    for(size_t i = 0; i < channel_count; i++){
        pthread_mutex_lock(&channel_list[i].channel->mutex);
        list_remove(channel_list[i].channel->semaphore_list, thread_nodes[i]);
        pthread_mutex_unlock(&channel_list[i].channel->mutex);
    }
}

// Creates a new channel with the provided size and returns it to the caller
channel_t* channel_create(size_t size)
{
    /* Store our channel in memory */
    channel_t* channel = (channel_t*)malloc(sizeof(channel_t));
    
    /* Initalize structures in our channel */
    channel->buffer = buffer_create(size);
    pthread_mutex_init(&channel->mutex, NULL);
    sem_init(&channel->send_semaphore, 0, 0);
    sem_init(&channel->rec_semaphore, 0, 0);
    channel->closed = 0;
    channel->semaphore_list = list_create();

    /* Return channel */
    return channel;
}

// Writes data to the given channel
// This is a blocking call i.e., the function only returns on a successful completion of send
// In case the channel is full, the function waits till the channel has space to write the new data
// Returns SUCCESS for successfully writing data to the channel,
// CLOSED_ERROR if the channel is closed, and
// GENERIC_ERROR on encountering any other generic error of any sort
enum channel_status channel_send(channel_t *channel, void* data)
{
    /* Lock our channel to only make it accessable by one thread */
    if(pthread_mutex_lock(&channel->mutex) != 0) return GENERIC_ERROR;

    /* Check if the channel is closed */
    if(channel->closed){
        if(pthread_mutex_unlock(&channel->mutex) != 0) return GENERIC_ERROR;
        return CLOSED_ERROR;
    }

    /* Check the status of the buffer and block while it's full */
    while(buffer_current_size(channel->buffer) >= buffer_capacity(channel->buffer)){
        if(pthread_mutex_unlock(&channel->mutex) != 0) return GENERIC_ERROR;

        if(sem_wait(&channel->send_semaphore) != 0) return GENERIC_ERROR;

        if(pthread_mutex_lock(&channel->mutex) != 0) return GENERIC_ERROR;

        if(channel->closed == 1) {
            if(sem_post(&channel->send_semaphore) != 0) return GENERIC_ERROR;
            if(pthread_mutex_unlock(&channel->mutex) != 0) return GENERIC_ERROR;
            return CLOSED_ERROR;
        }
    }

    /* Buffer is not full so send message */
    buffer_add(channel->buffer, data);

    /* Unlock our channel to make it accessable by other threads */
    if(sem_post(&channel->rec_semaphore) != 0) return GENERIC_ERROR;

    /* Notify semaphores in list */
    notify_semaphores(channel->semaphore_list);

    if(pthread_mutex_unlock(&channel->mutex) != 0) return GENERIC_ERROR;

    return SUCCESS;
}

// Reads data from the given channel and stores it in the function's input parameter, data (Note that it is a double pointer)
// This is a blocking call i.e., the function only returns on a successful completion of receive
// In case the channel is empty, the function waits till the channel has some data to read
// Returns SUCCESS for successful retrieval of data,
// CLOSED_ERROR if the channel is closed, and
// GENERIC_ERROR on encountering any other generic error of any sort
enum channel_status channel_receive(channel_t* channel, void** data)
{
    /* Lock our channel to only make it accessable by one thread */
    if(pthread_mutex_lock(&channel->mutex) != 0) return GENERIC_ERROR;

    /* Check if the channel is closed */
    if(channel->closed == 1){
        if(pthread_mutex_unlock(&channel->mutex) != 0) return GENERIC_ERROR;
        return CLOSED_ERROR;
    }

    /* Check the status of the buffer and block while it's empty */
    while(buffer_current_size(channel->buffer) == 0){
        if(pthread_mutex_unlock(&channel->mutex) != 0) return GENERIC_ERROR;

        if(sem_wait(&channel->rec_semaphore) != 0) return GENERIC_ERROR;

        if(pthread_mutex_lock(&channel->mutex) != 0) return GENERIC_ERROR;

        if(channel->closed == 1) {
            if(pthread_mutex_unlock(&channel->mutex) != 0) return GENERIC_ERROR;
            if(sem_post(&channel->rec_semaphore) != 0) return GENERIC_ERROR;
            return CLOSED_ERROR;
        }
    }

    /* Buffer is not empty so retrieve */
    buffer_remove(channel->buffer, data);

    /* Unlock our channel and update semaphore */
    if(sem_post(&channel->send_semaphore) != 0) return GENERIC_ERROR;

    /* Notify select function if needed */
    notify_semaphores(channel->semaphore_list);

    if(pthread_mutex_unlock(&channel->mutex) != 0) return GENERIC_ERROR;
    
    return SUCCESS;
}

// Writes data to the given channel
// This is a non-blocking call i.e., the function simply returns if the channel is full
// Returns SUCCESS for successfully writing data to the channel,
// CHANNEL_FULL if the channel is full and the data was not added to the buffer,
// CLOSED_ERROR if the channel is closed, and
// GENERIC_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_send(channel_t* channel, void* data)
{
    /* Lock our channel to only make it accessable by one thread */
    if(pthread_mutex_lock(&channel->mutex) != 0) return GENERIC_ERROR;

    /* Check if the channel is closed */
    if(channel->closed == 1){
        if(pthread_mutex_unlock(&channel->mutex) != 0) return GENERIC_ERROR;
        return CLOSED_ERROR;
    }

    /* Check the status of the buffer and block while it's full */
    if(buffer_current_size(channel->buffer) >= buffer_capacity(channel->buffer)){
        if(pthread_mutex_unlock(&channel->mutex) != 0) return GENERIC_ERROR;
        return CHANNEL_FULL;
    }

    /* Buffer is not full so send message */
    buffer_add(channel->buffer, data);

    /* Notifys a blocking receive that a message has been added */
    if(sem_post(&channel->rec_semaphore) != 0) return GENERIC_ERROR;

    /* Notify select function if needed */
    notify_semaphores(channel->semaphore_list);

    /* Unlock our channel to make it accessable by other threads */
    if(pthread_mutex_unlock(&channel->mutex) != 0) return GENERIC_ERROR;

    return SUCCESS;
}

// Reads data from the given channel and stores it in the function's input parameter data (Note that it is a double pointer)
// This is a non-blocking call i.e., the function simply returns if the channel is empty
// Returns SUCCESS for successful retrieval of data,
// CHANNEL_EMPTY if the channel is empty and nothing was stored in data,
// CLOSED_ERROR if the channel is closed, and
// GENERIC_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_receive(channel_t* channel, void** data)
{
    /* Lock our channel to only make it accessable by one thread */
    if(pthread_mutex_lock(&channel->mutex) != 0) return GENERIC_ERROR;

    /* Check if the channel is closed */
    if(channel->closed == 1){
        if(pthread_mutex_unlock(&channel->mutex) != 0) return GENERIC_ERROR;
        return CLOSED_ERROR;
    }

    /* Check the status of the buffer and block while it's empty */
    if(buffer_current_size(channel->buffer) == 0){
        if(pthread_mutex_unlock(&channel->mutex) != 0) return GENERIC_ERROR;
        return CHANNEL_EMPTY;
    }

    /* Buffer is not empty so retrieve */
    buffer_remove(channel->buffer, data);

    /* Notifys a blocking send that a message has been removed */
    if(sem_post(&channel->send_semaphore) != 0) return GENERIC_ERROR;

    /* Notify select function if needed */
    notify_semaphores(channel->semaphore_list);

    /* Unlock our channel */
    if(pthread_mutex_unlock(&channel->mutex) != 0) return GENERIC_ERROR;
    
    return SUCCESS;
}

// Closes the channel and informs all the blocking send/receive/select calls to return with CLOSED_ERROR
// Once the channel is closed, send/receive/select operations will cease to function and just return CLOSED_ERROR
// Returns SUCCESS if close is successful,
// CLOSED_ERROR if the channel is already closed, and
// GENERIC_ERROR in any other error case
enum channel_status channel_close(channel_t* channel)
{
    /* Lock the channel to prevent race conditions */
    if(pthread_mutex_lock(&channel->mutex) != 0) return GENERIC_ERROR;
    
    /* Check if channel is already closed */
    if(channel->closed){
        if(pthread_mutex_unlock(&channel->mutex) != 0) return GENERIC_ERROR;
        return CLOSED_ERROR;
    } else {
        channel->closed = 1;
    }

    /* Signal receive and send incase there are threads that are stuck waiting */
    if (sem_post(&channel->send_semaphore) != 0 || sem_post(&channel->rec_semaphore) != 0) {
        /* Unlock channel */
        if(pthread_mutex_unlock(&channel->mutex) != 0) return GENERIC_ERROR;
        return GENERIC_ERROR;
    }

    /* Notify select function if needed */
    notify_semaphores(channel->semaphore_list);

    /* Unlock channel */
    if(pthread_mutex_unlock(&channel->mutex) != 0) return GENERIC_ERROR;

    return SUCCESS;
}

// Frees all the memory allocated to the channel
// The caller is responsible for calling channel_close and waiting for all threads to finish their tasks before calling channel_destroy
// Returns SUCCESS if destroy is successful,
// DESTROY_ERROR if channel_destroy is called on an open channel, and
// GENERIC_ERROR in any other error case
enum channel_status channel_destroy(channel_t* channel)
{
    /* Check if channel is open */
    if(!channel->closed){
        return DESTROY_ERROR;
    }
    
    /* Free structures in our channel */    
    if(pthread_mutex_destroy(&channel->mutex) != 0) return GENERIC_ERROR;

    if(sem_destroy(&channel->send_semaphore) != 0) return GENERIC_ERROR;

    if(sem_destroy(&channel->rec_semaphore) != 0) return GENERIC_ERROR;

    buffer_free(channel->buffer);

    list_destroy(channel->semaphore_list);

    free(channel); 

    return SUCCESS;
}

// Takes an array of channels (channel_list) of type select_t and the array length (channel_count) as inputs
// This API iterates over the provided list and finds the set of possible channels which can be used to invoke the required operation (send or receive) specified in select_t
// If multiple options are available, it selects the first option and performs its corresponding action
// If no channel is available, the call is blocked and waits till it finds a channel which supports its required operation
// Once an operation has been successfully performed, select should set selected_index to the index of the channel that performed the operation and then return SUCCESS
// In the event that a channel is closed or encounters any error, the error should be propagated and returned through select
// Additionally, selected_index is set to the index of the channel that generated the error
enum channel_status channel_select(select_t* channel_list, size_t channel_count, size_t* selected_index)
{
    /* Operation status for each channel operation */
    enum channel_status op_status;
    /* Semaphore that will be signaled if a channel has been updated */
    sem_t semaphore;
    /* Initalize the semaphore */
    sem_init(&semaphore, 0, 0);
    /* Create an array of nodes */
    list_node_t *thread_nodes[channel_count];
    /* Add semaphore to our channels semaphores list */
    insert_list_semaphore(channel_list, channel_count, &semaphore, thread_nodes);
    /* operation tracker */
    int operation = 0;

    /* Loop through each channel and try to perform an operation */
    while(operation == 0){
        for(size_t i = 0; i < channel_count; i++){
            /* Attempt to perform the operation */
            if(channel_list[i].dir == RECV){ 
                op_status = channel_non_blocking_receive(channel_list[i].channel, &channel_list[i].data);
            } else {
                op_status = channel_non_blocking_send(channel_list[i].channel, channel_list[i].data);
            }

            /* Evaluate how the operation went */
            if(op_status == CLOSED_ERROR || op_status == GENERIC_ERROR || op_status == SUCCESS){
                *selected_index = i;
                operation = 1;
                break;
            } else {
                if (i == channel_count - 1){ /* Checks if we looped through every channel */
                    /* Wait for a signal on the semaphore */
                    if(sem_wait(&semaphore) != 0) return GENERIC_ERROR;
                }
            }
        }
    }
    
    remove_list_semaphore(channel_list, channel_count, thread_nodes);
    if(sem_destroy(&semaphore) != 0) return GENERIC_ERROR;
    return op_status;
}
