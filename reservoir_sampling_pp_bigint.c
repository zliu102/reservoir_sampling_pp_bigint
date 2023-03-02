/*-------------------------------------------------------------------------
 *
 * binary_search.c
 *	  PostgreSQL type definitions for BINARY_SEARCHs
 *
 * Author:	Boris Glavic
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/binary_search/binary_search.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "reservoir_sampling_pp_bigint.h"

#include "c.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "nodes/makefuncs.h"
#include "parser/parse_func.h"
#include "utils/array.h"
#include "utils/arrayaccess.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/typcache.h"
#include "utils/varbit.h"
#include "postgres.h"
#include <limits.h>
#include "catalog/pg_type.h"
//#include "/home/oracle/datasets/postgres11ps/postgres-pbds/contrib/intarray/_int.h"

PG_MODULE_MAGIC;

typedef struct state_c
{
	ArrayType *reservoir;
        int32 poscnt;
        int32 reservoir_size; 
} state_c;

#define MAX_GROUPS 787
#define RES_SIZE 3
static ArrayType *MyNew_intArrayType(int num);
static bool initialized = false;
//static int lastgroup = -1;
//static state_c **states = NULL;
static state_c *states_2[MAX_GROUPS];
//static int64 *results = NULL;
static int64 results[MAX_GROUPS][RES_SIZE] = {0};
//static ArrayType **res = NULL;
//static Datum charToInt(const char *addr);
//static void intToChar(unsigned int hex, char* str)



PG_FUNCTION_INFO_V1(res_trans_crimes_c_pp_bigint);
Datum
res_trans_crimes_c_pp_bigint(PG_FUNCTION_ARGS)
{
    
    //bytea  *addr = (bytea *) PG_GETARG_BYTEA_P(0);
    //elog(INFO, "addr is %p",addr);
    int64 lastgroup = PG_GETARG_INT64(0);
    int64 group_index = PG_GETARG_INT64(1);
    int64 newsample = PG_GETARG_INT64(2);
    elog(INFO, "lastgroup is %ld",lastgroup);
    elog(INFO, "group_index is %ld",group_index);
    elog(INFO, "newsample is %ld",newsample);   
    //state_c *s = palloc (sizeof(state_c));
    //if (states == NULL) {
     //   states = (state_c **) palloc0(MAX_GROUPS * sizeof(state_c *));
      //  elog(INFO, "states is %p",states); 
    //}
    if (!initialized) {
        
        for (int i = 0;i<MAX_GROUPS;i++){
            states_2[i]= (state_c *) palloc0(sizeof(state_c));
            //elog(INFO, "states_2 is %p",states_2[i]); 
        }
        initialized = true;
        //elog(INFO, "states_2 is %p",states_2); 
    }
    

    if (lastgroup != group_index){
          
            state_c *st0 = (state_c *) palloc (sizeof(state_c));
            elog(INFO, "st0 is %p",st0);   
            //addr = (bytea *) palloc(sizeof(st0) + sizeof(bytea));
            //SET_VARSIZE(addr,sizeof(st0)+sizeof(bytea));      
            st0->poscnt = 1;
            st0->reservoir_size = RES_SIZE;
            //ArrayType *a = MyNew_intArrayType(RES_SIZE);
            //elog(INFO, "a is %p",a);   
            //st0->reservoir = a;
            //states[group_index - 1] = st0;
            states_2[group_index - 1] = st0;
            //todo res[group_index - 1] = 
            //elog(INFO, "st0 poscnt is %d,reservoir_size is %d",st0->poscnt,st0->reservoir_size);
        
            //memcpy(VARDATA(addr), &st0, sizeof(void *));
            //memcpy(VARDATA(addr), &st0, sizeof(void *));
            //initialized = true;

    }
        
        //void **new_ptr = (void **) VARDATA(addr);
       // s= (state_c *) states[group_index-1];
        state_c *s =(state_c *) states_2[group_index - 1];
        elog(INFO, "s is %p",s);
        elog(INFO, "s poscnt is %d,reservoir_size is %d",s->poscnt,s->reservoir_size);
        if(s->poscnt <= s->reservoir_size){
        //    elog(INFO, "case 1");
            int32 p = s->poscnt;
            //int64 *dr = (int64 *) ARR_DATA_PTR(s->reservoir);
            //dr[p-1] = newsample;
            results[group_index-1][p-1] = newsample;
            
            s->poscnt ++;
        }else{
        //    elog(INFO, "case 2");
            int32 pos = rand() % s->poscnt ;
        //    elog(INFO, "pos is %d",pos);//0 - postcnt -1
            if(pos < s->reservoir_size){
                //int64 *dr = (int64 *) ARR_DATA_PTR(s->reservoir);
                //ådr[pos] = newsample;
                results[group_index-1][pos] = newsample;
            }
            s->poscnt ++;
        }
        elog(INFO, "----------------"); 
        //lastgroup = group_index;
        //pfree(s);
        PG_RETURN_INT64(group_index);
}

PG_FUNCTION_INFO_V1(finalize_trans_crimes_c_pp_bigint);
Datum
finalize_trans_crimes_c_pp_bigint(PG_FUNCTION_ARGS)
{               

                ArrayType *result;
                Datum *elems;
                int i;
                //int num;
                //int64 *dr;

                //state_c *st = palloc (sizeof(state_c));
                //bytea  *addr = (bytea *) PG_GETARG_BYTEA_P(0);
                //void **new_ptr = (void **) VARDATA(addr);
                int64 group_index = PG_GETARG_INT64(0);
                //elog(INFO, "group_index is %ld",group_index);
                //st= (state_c *) states[group_index - 1];
                //state_c *st= (state_c *) states_2[group_index - 1];
                //elog(INFO, "inner_array 0,1,2 is %ld,%ld,%ld",results[group_index-1][0],results[group_index-1][1],results[group_index-1][2]);
                //elog(INFO, "st is %p",st);
                //elog(INFO, "st poscnt is %d,reservoir_size is %d",st->poscnt,st->reservoir_size);
                //num = st->reservoir_size;
                //dr = (int64 *) ARR_DATA_PTR(st->reservoir); 
                
                elems = (Datum *)palloc(RES_SIZE * sizeof(Datum));
                
                for (i = 0; i < RES_SIZE; i++) {
                    elems[i] = Int64GetDatum(results[group_index-1][i]); 
                    //elog(INFO, "dr[%d] is %ld",i,dr[i]);
                    //elog(INFO, "elems[%d] is %ld",i,elems[i]);
                }
                //int nbytes = ARR_OVERHEAD_NONULLS(1) + sizeof(int) * num;
                //result = (ArrayType *) palloc0(nbytes);
                
                result = construct_array((Datum *)elems, RES_SIZE , INT8OID, sizeof(int64), true, 'i');
                /*if (ARR_NDIM(result) != 1 ){
                     elog(INFO, "yes1");
                 }
                if (ARR_HASNULL(result)) {
                    elog(INFO, "yes2");
                }
                if(ARR_ELEMTYPE(result) != INT8OID){
                    elog(INFO, "yes3");
                }
                if (result && ARR_ELEMTYPE(result) == INT8OID) {
                    elog(INFO, "yes4");
                }*/
               
                //pfree(addr);
                //initialized = false;
                //elog(INFO, "before retrun initialized is %d",initialized);
                PG_RETURN_ARRAYTYPE_P(result);
                //PG_RETURN_ARRAYTYPE_P(st->reservoir); 
}
static
ArrayType *
MyNew_intArrayType(int num)
{
        ArrayType  *r;
        int nbytes;

        /* if no elements, return a zero-dimensional array */
        if (num <= 0)
        {
                Assert(num == 0);
                r = construct_empty_array(INT8OID);
                return r;
        }

        nbytes = ARR_OVERHEAD_NONULLS(1) + sizeof(int) * num;

        r = (ArrayType *) palloc0(nbytes);

        SET_VARSIZE(r, nbytes);
        ARR_NDIM(r) = 1;
        r->dataoffset = 0;                      /* marker for no null bitmap */
        ARR_ELEMTYPE(r) = INT8OID;
        ARR_DIMS(r)[0] = num;
        ARR_LBOUND(r)[0] = 1;

        return r;
}
/*static Datum
charToInt(const char *str)
{
    int result = 0;
    int length = strlen(str);
    int i;
    for (i = 0; i < length; i++) {
        char c = str[i];
        int value;
        if (c >= '0' && c <= '9') {
            value = c - '0';
        } else if (c >= 'a' && c <= 'f') {
            value = 10 + c - 'a';
        } else if (c >= 'A' && c <= 'F') {
            value = 10 + c - 'A';
        } else {
            return 0;
        }
        result = result * 16 + value;
    }
    return result;
}

static
void intToChar(unsigned int hex, char* str) {
    sprintf(str, "%x", hex);
}


PG_FUNCTION_INFO_V1(res_tras_crimes2);
Datum
res_tras_crimes2_c(PG_FUNCTION_ARGS)
{

		//state_c *d1 = malloc(sizeof(state_c));
		//struct state_c st;
	//	st = (state_c *)PG_GETARG_DATUM(0);
        state_c *st = (state_c *)PG_GETARG_DATUM(0);
        int64 newsample = PG_GETARG_INT64(1);

        memset(&st, 0, sizeof(struct state_c));
        if (memcmp(&st, &st, sizeof(struct state_c)) == 0) {
        	int64 r[] = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
        	int64 *a = r;
        	st.poscnt = 1;
        	st.reservoir_size = 100;
        	st.reservoir = a;
        }
        if (st == NULL) {
                int64 r[] = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
                int64 *a = r;
                st->poscnt = 1;
                st->reservoir_size = 100;
                st->reservoir = a;
        }
        if(st->poscnt <= st->reservoir_size){
        	int32 p = st->poscnt;
        	*(st->reservoir+p-1) = newsample;
        	st->poscnt ++;

        }else{
        	int32 pos = rand() % st->poscnt ; //0 - postcnt -1
        	if(pos < st->reservoir_size){
        		*(st->reservoir+pos) = newsample;
        	}
        	st->poscnt ++;
        }
        PG_RETURN_DATUM((Datum) st);
}

PG_FUNCTION_INFO_V1(finalize_trans_crimes2);
Datum
finalize_trans_crimes2_c(PG_FUNCTION_ARGS)
{

	state_c *st = (state_c *) PG_GETARG_DATUM(0);
	PG_RETURN_ARRAYTYPE_P(st->reservoir);
}*/
