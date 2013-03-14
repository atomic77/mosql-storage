#ifndef _MGET_RESULT_H_
#define _MGET_RESULT_H_


#ifdef __cplusplus
extern "C" {
#endif

typedef struct mget_result {
    int count;
    struct evbuffer* buffer;
} mget_result;


/**
  Consumes one value from the given mget_result.
  
  @param a pointer to a memory are in which we can store the value
	@return number of bytes written in v, 0 if the key does not exist,
	-1 if an error occured.
*/
int mget_result_consume(mget_result* res, void* v);


/**
  Returns the number of values left in a mget_result.
  
  @param res the mget_result
  @return the number of values left
*/
int mget_result_count(mget_result* res);


/**
  Free the memory pointed by the given mget_result
*/
void mget_result_free(mget_result* res);



#ifdef __cplusplus
}
#endif

#endif
