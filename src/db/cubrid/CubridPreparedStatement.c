#include "Config.h"

#include <stdio.h>
#include <string.h>
#include <cas_cci.h>

#include "ResultSet.h"
#include "CubridResultSet.h"
#include "PreparedStatementDelegate.h"
#include "CubridPreparedStatement.h"

/**
 * Implementation of the PreparedStatement/Delegate interface for cubrid.
 *
 * @file
 */


/* ----------------------------------------------------------- Definitions */

#define CUBRID_OK 0

 const struct Pop_T cubridpops = {
        "cubrid",
        CubridPreparedStatement_free,
        CubridPreparedStatement_setString,
        CubridPreparedStatement_setInt,
        CubridPreparedStatement_setLLong,
        CubridPreparedStatement_setDouble,
        CubridPreparedStatement_setBlob,
        CubridPreparedStatement_execute,
        CubridPreparedStatement_executeQuery
};

typedef struct param_t {
        union {
                long integer;
                long long int llong;
                double real;
        } type;
        long length;
        int is_null;
        void *buffer;
        int index;
        T_CCI_A_TYPE valueType;
        T_CCI_U_TYPE bindType;
} *param_t;

typedef int cubrid_db_conn_t;
typedef int cubrid_db_req_t;

#define T PreparedStatementDelegate_T
struct T {
    int maxRows;
    int lastError;
    int paramCount;
    param_t params;
	cubrid_db_conn_t db;
	cubrid_db_req_t req;
};

#define TEST_INDEX \
        int i; assert(P); i = parameterIndex - 1; if (P->paramCount <= 0 || \
        i < 0 || i >= P->paramCount) THROW(SQLException, "Parameter index is out of range"); 

extern const struct Rop_T cubridrops;

/* ----------------------------------------------------- Private methods */

#ifndef OSX
int64_t ntohi64( int64_t value )
{
    return value;
}
#endif

/* ----------------------------------------------------- Protected methods */


#ifdef PACKAGE_PROTECTED
#pragma GCC visibility push(hidden)
#endif

T CubridPreparedStatement_new(int conn, int req, int maxRows) {
        
    T P;
    int bindNumber = 0;

    bindNumber = cci_get_bind_num(req);
    if (bindNumber == CCI_ER_REQ_HANDLE) {
    	bindNumber = 0;
    }

    NEW(P);
    P->req = req;
    P->db = conn;
    P->maxRows = maxRows;
    P->paramCount = bindNumber;

    if (P->paramCount > 0) {
        P->params = CALLOC(P->paramCount, sizeof(struct param_t));
    }
    
    P->lastError = CCI_ER_NO_ERROR;
    return P;
}


void CubridPreparedStatement_free(T *P) {
    assert(P && *P);

    cci_close_req_handle((*P)->req);
    (*P)->req = -1;

    for (int i = 0; i < (*P)->paramCount; ++i) {
    	if ((*P)->params[i].valueType == CCI_A_TYPE_BLOB) {
    		T_CCI_BLOB blob = (T_CCI_BLOB)((*P)->params[i].buffer);
    		cci_blob_free(blob);
            (*P)->params[i].buffer = NULL;
    	}
    }

    FREE((*P)->params);
	FREE(*P);
}


void CubridPreparedStatement_setString(T P, int parameterIndex, const char *x) {
    TEST_INDEX

    P->params[i].index = parameterIndex;
    P->params[i].bindType = CCI_U_TYPE_STRING;
    P->params[i].valueType = CCI_A_TYPE_STR;
    P->params[i].buffer = (void *)x;
        
    if (!x) {
        P->params[i].length = 0;
        P->params[i].is_null = 1;
    } else {
        P->params[i].length = strlen(x);
        P->params[i].is_null = 0;
    }
}


void CubridPreparedStatement_setInt(T P, int parameterIndex, int x) {
    TEST_INDEX

    P->params[i].index = parameterIndex;    
    P->params[i].type.integer = x;
    P->params[i].buffer = (void *)&(P->params[i].type.integer);
    P->params[i].bindType = CCI_U_TYPE_INT;
    P->params[i].valueType = CCI_A_TYPE_INT;
    P->params[i].is_null = 0;
}


void CubridPreparedStatement_setLLong(T P, int parameterIndex, long long int x) {
    TEST_INDEX
   
    P->params[i].index = parameterIndex; 
    P->params[i].type.llong = x;
    P->params[i].buffer = (void *)&(P->params[i].type.llong);
    P->params[i].bindType = CCI_U_TYPE_BIGINT;
    P->params[i].valueType = CCI_A_TYPE_BIGINT;
    P->params[i].is_null = 0;
}


void CubridPreparedStatement_setDouble(T P, int parameterIndex, double x) {
    TEST_INDEX
       
    P->params[i].index = parameterIndex; 
    P->params[i].type.real = x;
    P->params[i].buffer = (void *)&(P->params[i].type.real);
    P->params[i].bindType = CCI_U_TYPE_DOUBLE;
    P->params[i].valueType = CCI_A_TYPE_DOUBLE;
    P->params[i].is_null = 0;
}

void CubridPreparedStatement_setBlob(T P, int parameterIndex, const void *x, int size) {
    int res = 0;
    T_CCI_ERROR error;
    T_CCI_BLOB blob;

    TEST_INDEX
    
    P->params[i].index = parameterIndex; 
    P->params[i].bindType = CCI_U_TYPE_BLOB;
    P->params[i].valueType = CCI_A_TYPE_BLOB;
    
    if (!x) {
        P->params[i].length  = 0;
        P->params[i].is_null = 1;
    } else {
        P->params[i].length  = size;
        P->params[i].is_null = 0;
    }

    P->lastError = cci_blob_new(P->db, &blob, &error);
    if (P->lastError != CCI_ER_NO_ERROR) {
    	THROW(SQLException, "cci_blob_write(%d)", P->lastError);
    	return ;
    }
    
    res = cci_blob_write(P->db, blob, 0, size, x, &error);
    if (res < 0) {
    	THROW(SQLException, "cci_blob_write(%d)", P->lastError);
    	return ;
    }

    if (res != size) {
    	THROW(SQLException, "mismatch size (%d) <> (%d)", res, size);
    	return ;
    }

    P->params[i].buffer = (void *)blob;
}


void CubridPreparedStatement_execute(T P) {
    T_CCI_ERROR error;
    int res = 0;
    int n_executed;

    assert(P);

    for (int i = 0; i < P->paramCount; ++i) {
    	//DEBUG("CubridPreparedStatement_execute: %dth, %d\n", P->params[i].index, P->params[i].valueType);
        res = cci_bind_param(P->req, 
    		                 P->params[i].index,
    		                 P->params[i].valueType, 
    		                 P->params[i].buffer, 
    		                 P->params[i].bindType,
    		                 CCI_BIND_PTR);
        if (res != CCI_ER_NO_ERROR) {
        	goto handle_error;
        }

    }

    n_executed = cci_execute(P->req, 0, 0, &error);
    if (n_executed < 0) {
        goto handle_error;
    }

    if (cci_end_tran(P->db, CCI_TRAN_COMMIT, &error) < 0) {
        goto handle_error;
    }

    return ;

handle_error:
    THROW(SQLException, "%s", error.err_msg);
}


ResultSet_T CubridPreparedStatement_executeQuery(T P) {
	T_CCI_ERROR error;
    int res = 0;
    int n_executed;

    assert(P);

    for (int i = 0; i < P->paramCount; ++i) {
    	//DEBUG("CubridPreparedStatement_execute: %dth, %d\n", P->params[i].index, P->params[i].valueType);
        res = cci_bind_param(P->req, 
    		                 P->params[i].index,
    		                 P->params[i].valueType, 
    		                 P->params[i].buffer, 
    		                 P->params[i].bindType,
    		                 CCI_BIND_PTR);
        if (res < 0) {
        	goto handle_error;
        }

    }

    n_executed = cci_execute(P->req, 0, 0, &error);
    if (n_executed < 0) {
        goto handle_error;
    }

    if (cci_end_tran(P->db, CCI_TRAN_COMMIT, &error) < 0) {
        goto handle_error;
    }

    return ResultSet_new(CubridResultSet_new(P->db, P->req, P->maxRows), (Rop_T)&cubridrops);

handle_error:
    THROW(SQLException, "%d", res);
    return NULL;
}


#ifdef PACKAGE_PROTECTED
#pragma GCC visibility pop
#endif