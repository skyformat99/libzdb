#include "Config.h"

#include <stdio.h>
#include <string.h>
#include <cas_cci.h>

#include "ResultSetDelegate.h"
#include "CubridResultSet.h"


/**
 * Implementation of the ResultSet/Delegate interface for Cubrid. 
 * Accessing columns with index outside range throws SQLException
 *
 * @file
 */


/* ------------------------------------------------------------- Definitions */

#define CUBRID_OK 0

#define MAX_CUBRID_CHAR_LEN   1073741823
#define MAX_LEN_INTEGER       (10 + 1)
#define MAX_LEN_SMALLINT      (5 + 1)
#define MAX_LEN_BIGINT        (19 + 1)
#define MAX_LEN_FLOAT         (14 + 1)
#define MAX_LEN_DOUBLE        (28 + 1)
#define MAX_LEN_MONETARY      (28 + 2)
#define MAX_LEN_DATE          10
#define MAX_LEN_TIME          8 
#define MAX_LEN_TIMESTAMP     23
#define MAX_LEN_DATETIME      MAX_LEN_TIMESTAMP
#define MAX_LEN_OBJECT        MAX_CUBRID_CHAR_LEN
#define MAX_LEN_SET           MAX_CUBRID_CHAR_LEN
#define MAX_LEN_MULTISET      MAX_CUBRID_CHAR_LEN
#define MAX_LEN_SEQUENCE      MAX_CUBRID_CHAR_LEN
#define MAX_LEN_LOB           MAX_CUBRID_CHAR_LEN

typedef struct
{
    char *type_name;
    T_CCI_U_TYPE cubrid_u_type;
    int len;
} DB_TYPE_INFO;

/* Define Cubrid supported date types */
static const DB_TYPE_INFO db_type_info[] = {
    {"NULL", CCI_U_TYPE_NULL, 0},
    {"UNKNOWN", CCI_U_TYPE_UNKNOWN, MAX_LEN_OBJECT},

    {"CHAR", CCI_U_TYPE_CHAR, -1},
    {"STRING", CCI_U_TYPE_STRING, -1},
    {"NCHAR", CCI_U_TYPE_NCHAR, -1},
    {"VARNCHAR", CCI_U_TYPE_VARNCHAR, -1},

    {"BIT", CCI_U_TYPE_BIT, -1},
    {"VARBIT", CCI_U_TYPE_VARBIT, -1},

    {"NUMERIC", CCI_U_TYPE_NUMERIC, -1},
    {"NUMBER", CCI_U_TYPE_NUMERIC, -1},
    {"INT", CCI_U_TYPE_INT, MAX_LEN_INTEGER},
    {"SHORT", CCI_U_TYPE_SHORT, MAX_LEN_SMALLINT},
    {"BIGINT", CCI_U_TYPE_BIGINT, MAX_LEN_BIGINT},
    {"MONETARY", CCI_U_TYPE_MONETARY, MAX_LEN_MONETARY},

    {"FLOAT", CCI_U_TYPE_FLOAT, MAX_LEN_FLOAT},
    {"DOUBLE", CCI_U_TYPE_DOUBLE, MAX_LEN_DOUBLE},

    {"DATE", CCI_U_TYPE_DATE, MAX_LEN_DATE},
    {"TIME", CCI_U_TYPE_TIME, MAX_LEN_TIME},
    {"DATETIME", CCI_U_TYPE_DATETIME, MAX_LEN_DATETIME},
    {"TIMESTAMP", CCI_U_TYPE_TIMESTAMP, MAX_LEN_TIMESTAMP},

    {"SET", CCI_U_TYPE_SET, MAX_LEN_SET},
    {"MULTISET", CCI_U_TYPE_MULTISET, MAX_LEN_MULTISET},
    {"SEQUENCE", CCI_U_TYPE_SEQUENCE, MAX_LEN_SEQUENCE},
    {"RESULTSET", CCI_U_TYPE_RESULTSET, -1},

    {"OBJECT", CCI_U_TYPE_OBJECT, MAX_LEN_OBJECT},
    {"BLOB", CCI_U_TYPE_BLOB, MAX_LEN_LOB},
    {"CLOB", CCI_U_TYPE_CLOB, MAX_LEN_LOB}
};


const struct Rop_T cubridrops = {
	"cubrid",
    CubridResultSet_free,
    CubridResultSet_getColumnCount,
    CubridResultSet_getColumnName,
    CubridResultSet_next,
    CubridResultSet_getColumnSize,
    CubridResultSet_getString,
    CubridResultSet_getBlob,
};

typedef int cubrid_db_conn_t;
typedef int cubrid_db_req_t;

#define T ResultSetDelegate_T
struct T {
    int stop;
    int maxRows;
    int lastError;
	int currentRow;
	int columnCount;
	T_CCI_CUBRID_STMT stmt_type;
	T_CCI_COL_INFO *column_info;
	char **columnsData;
	cubrid_db_conn_t db;
	cubrid_db_req_t req;
};

#define TEST_INDEX \
        int i; assert(R);i = columnIndex-1; if (R->columnCount <= 0 || \
        i < 0 || i >= R->columnCount) { THROW(SQLException, "Column index is out of range"); }  

/* ------------------------------------------------------- Private methods */

static int get_cubrid_u_type_len(T_CCI_U_TYPE type) {
    int i;
    int size = sizeof(db_type_info) / sizeof(db_type_info[0]);

    DB_TYPE_INFO type_info;

    for (i = 0; i < size; i++) {
	    type_info = db_type_info[i];
	    if (type == type_info.cubrid_u_type) {
	        return type_info.len;
	    }
    }

    return 0;
}

/*
static char* get_cubrid_u_typename(T_CCI_U_TYPE type) {
    int i;
    int size = sizeof(db_type_info) / sizeof(db_type_info[0]);

    DB_TYPE_INFO type_info;

    for (i = 0; i < size; i++) {
	    type_info = db_type_info[i];
	    if (type == type_info.cubrid_u_type) {
	        return type_info.type_name;
	    }
    }

    return "";
}
*/

/* ----------------------------------------------------- Protected methods */


#ifdef PACKAGE_PROTECTED
#pragma GCC visibility push(hidden)
#endif

T CubridResultSet_new(int conn, int req, int maxRows) {

	T R;
	T_CCI_COL_INFO *res_col_info;
    T_CCI_CUBRID_STMT stmt_type;
    int col_count = 0;

    assert(req > 0);

	res_col_info = cci_get_result_info(req, &stmt_type, &col_count);
    if (!res_col_info) {
        DEBUG("%s(%d): cci_get_result_info fail\n", __FILE__, __LINE__);
        return NULL;
    }

    NEW(R);
	R->req = req;
    R->maxRows = maxRows;
    R->columnCount = col_count;
    R->stmt_type = stmt_type;
    R->column_info = res_col_info;
    R->db = conn;
    R->lastError = CCI_ER_NO_ERROR;
    
    if (R->columnCount <= 0) {
        DEBUG("Warning: column error");
        R->stop = true;
    } else {
    	R->columnsData = CALLOC(R->columnCount, sizeof(char *));
    }
	
	return R;
}

void CubridResultSet_free(T *R) {
	assert(R && *R);

    (*R)->req = -1;

    for (int i = 0; i < (*R)->columnCount; ++i) {
   	    FREE((*R)->columnsData[i]);	
    }       

    FREE((*R)->columnsData);
    FREE(*R);
}

int CubridResultSet_getColumnCount(T R) {
	assert(R);
	return R->columnCount;
}

const char *CubridResultSet_getColumnName(T R, int column) {
	assert(R);

	if (R->columnCount <= 0 ||
	   column < 1           ||
	   column > R->columnCount) {
		return NULL;
	}

	return CCI_GET_RESULT_INFO_NAME(R->column_info, column);
}

int CubridResultSet_next(T R) {
	T_CCI_ERROR error;
    int res = 0;

	assert(R);
       
    if (R->stop) {
    	return false;
    }
        
    if (R->maxRows && (R->currentRow++ >= R->maxRows)) {
        R->stop = true;
        return false;
    }

    res = cci_cursor(R->req, 1, CCI_CURSOR_CURRENT, &error);
    if (res == CCI_ER_NO_MORE_DATA) {
        R->stop = true;
        return false;
    } else if (res < 0) {
        goto handle_error;
    }

    res = cci_fetch(R->req, &error);
    if (res < 0)
    {
        DEBUG("fetch error (%d)\n", res);
        goto handle_error;
    }

    return ((R->lastError == CCI_ER_NO_ERROR) || (R->lastError == CCI_ER_NO_MORE_DATA));

handle_error:
    R->stop = true;
    R->lastError = error.err_code;
    THROW(SQLException, "CubridResultSet_next:%s", error.err_msg);
    return false;
}


long CubridResultSet_getColumnSize(T R, int columnIndex) {
    T_CCI_U_TYPE type;
    long len = 0;
    
    TEST_INDEX

    if (!CCI_GET_RESULT_INFO_IS_NON_NULL(R->column_info, columnIndex)) {
    	return 0;
    }

    type = CCI_GET_COLLECTION_DOMAIN(CCI_GET_RESULT_INFO_TYPE(R->column_info, columnIndex));

    if ((len = get_cubrid_u_type_len(type)) == -1) {
	    
	    len = CCI_GET_RESULT_INFO_PRECISION(R->column_info, columnIndex); 
	     
	    if (type == CCI_U_TYPE_NUMERIC) {
	        len += 2; /* "," + "-" */
	    }
    }

    if (CCI_IS_COLLECTION_TYPE(CCI_GET_RESULT_INFO_TYPE(R->column_info, columnIndex))) {
	   len = MAX_LEN_SET;
    }

    return len;
}


const char *CubridResultSet_getString(T R, int columnIndex) {

	char *buffer = NULL, *value_buffer = NULL;
	int indicator = 0;
	int res = 0;
	T_CCI_U_TYPE type;

    TEST_INDEX

    FREE(R->columnsData[i]);
    R->columnsData[i] = NULL;

    type = CCI_GET_RESULT_INFO_TYPE(R->column_info, columnIndex);

    /* type 이 CCI_A_TYPE_STR 인 경우 : NULL 이면 -1을 반환하고, NULL 이 아니면 value 에 저장된 문자열의 길이를 반환 */
    res = cci_get_data(R->req, columnIndex, CCI_A_TYPE_STR, &buffer, &indicator);
    if (res != CCI_ER_NO_ERROR) {
    	R->lastError = res;
        THROW(SQLException, "cci_get_data(%d)", res);
    }

    if (indicator < 0) {
    	return NULL;
    }

    R->columnsData[i] = ALLOC(indicator + 1);
    R->columnsData[i][indicator] = 0;
    Str_copy(R->columnsData[i], buffer, indicator);

    FREE(value_buffer);

    //DEBUG("%s =>[%s]\n", get_cubrid_u_typename(type), R->columnsData[i]);
    return R->columnsData[i];
}

const void *CubridResultSet_getBlob(T R, int columnIndex, int *size) {

	T_CCI_BLOB blob;
	T_CCI_ERROR error;
	long long blobSize = 0;
    int indicator = 0;
	int res = 0;
    
    TEST_INDEX

    FREE(R->columnsData[i]);
    R->columnsData[i] = NULL;

    /* type 이 CCI_A_TYPE_STR 이 아닌 경우 : NULL 이면 -1을 반환하고, NULL 이 아니면 0을 반환 */
    res = cci_get_data(R->req, columnIndex, CCI_A_TYPE_BLOB, (void *)&blob, &indicator);
    if (res < 0) {
    	R->lastError = res;
        THROW(SQLException, "cci_get_data(%d)", res);
    }

    if (indicator < 0) {
    	return NULL;
    } 

    blobSize = cci_blob_size(blob);
    if (blobSize < 0) {
    	R->lastError = blobSize;
        THROW(SQLException, "get blob size error %d", blobSize);
    }

    //DEBUG("blob size : %d\n", blobSize);
    if (blobSize == 0) {
    	return NULL;
    }

    R->columnsData[i] = ALLOC(blobSize);
    R->lastError = cci_blob_read(R->db, blob, 0, blobSize, R->columnsData[i], &error);
    
    cci_blob_free(blob);

    if (R->lastError < 0) {
    	THROW(SQLException, "GetColumnsData error:%d", error.err_code);
    }

    *size = blobSize;
  
    return R->columnsData[i];
}

#ifdef PACKAGE_PROTECTED
#pragma GCC visibility pop
#endif
