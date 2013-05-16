#include "Config.h"

#include <stdio.h>
#include <string.h>
#include <cas_cci.h>

#include "URL.h"
#include "ResultSet.h"
#include "StringBuffer.h"
#include "PreparedStatement.h"
#include "CubridResultSet.h"
#include "CubridPreparedStatement.h"
#include "ConnectionDelegate.h"
#include "CubridConnection.h"

#ifdef WIN32
    #include <windows.h>
#else
    #include <dlfcn.h>
#endif

#define T ConnectionDelegate_T
#define ERROR(e) do {*error = Str_dup(e); goto error;} while (0)
#define SET_LAST_ERROR(C, cci_error) \
    	do { \
    		C->lastError = cci_error.err_code; \
    	    StringBuffer_clear(C->sb); \
            StringBuffer_append(C->sb, "%s", cci_error.err_msg); \
        } while(0); 
#define CUBRID_OK 0

typedef int cubrid_db_conn_t;
typedef int cubrid_db_req_t;

const struct Cop_T cubridcops = {
        "cubrid",
        CubridConnection_onstop,
        CubridConnection_new,
        CubridConnection_free,
        CubridConnection_setQueryTimeout,
        CubridConnection_setMaxRows,
        CubridConnection_ping,
        CubridConnection_beginTransaction,
        CubridConnection_commit,
        CubridConnection_rollback,
        CubridConnection_lastRowId,
        CubridConnection_rowsChanged,
        CubridConnection_execute,
        CubridConnection_executeQuery,
        CubridConnection_prepareStatement,
        CubridConnection_getLastError
};

struct T {
    URL_T url;
	cubrid_db_conn_t db;
	int maxRows;
	int timeout;
	int lastError;
    StringBuffer_T sb;
};

extern const struct Rop_T cubridrops;
extern const struct Pop_T cubridpops;

static void *libcascci = NULL;
typedef int (*CCI_GET_LAST_INSERT_ID) (int con, void *buff,
                                       T_CCI_ERROR * err_buf);
static CCI_GET_LAST_INSERT_ID cci_get_last_insert_id_fp = NULL;

/* ------------------------------------------------------- Private methods */
static cubrid_db_conn_t doConnect(URL_T url, char **error) {
    int port;
    cubrid_db_conn_t conn = 0;
    volatile int connectTimeout = SQL_DEFAULT_TCP_TIMEOUT;
    char *user, *password, *host, *database, *timeout;
    T_CCI_ERROR cci_error;
    char buffer[1024];
    char *unix_socket = (char *)URL_getParameter(url, "unix-socket");

    if (!(user = (char *)URL_getUser(url))) {
    	if (!(user = (char *)URL_getParameter(url, "user")))
            ERROR("no username specified in URL");
    }
            
    if (!(password = (char *)URL_getPassword(url))) {
    	if (! (password = (char *)URL_getParameter(url, "password")))
            ERROR("no password specified in URL");
    }

    if (unix_socket) {
		host = "localhost"; // Make sure host is localhost if unix socket is to be used
    } else if (! (host = (char *)URL_getHost(url))) {
    	ERROR("no host specified in URL");
    }
       
    if ((port = URL_getPort(url)) <= 0) {
    	ERROR("no port specified in URL");
    }
        
    if (!(database = (char *)URL_getPath(url))) {
    	ERROR("no database specified in URL");
    } else {
    	database++;
    }
          
    if ((timeout = (char *)URL_getParameter(url, "connect-timeout"))) {
        TRY 
             connectTimeout = Str_parseInt(timeout); 
        ELSE 
              ERROR("invalid connect timeout value");
        END_TRY;
    }

    /* Connect */
    //printf("%s:%d, %s %s %s\n", host, port, database, user, password);
    conn = cci_connect(host, port, database, user, password);
    if (conn >= 0) {
        return conn;
    }

    cci_get_error_msg(conn, &cci_error, buffer, sizeof(buffer)/sizeof(char));
    *error = Str_dup(buffer);   

error:
    return -1;   
}

void initCubrid() {

#ifdef WIN32
	libcascci = LoadLibrary("cascci.dll");
    if (libcascci) {
    	cci_get_last_insert_id_fp = (CCI_GET_LAST_INSERT_ID)GetProcAddress(libcascci, "cci_get_last_insert_id");
    }
#else
    libcascci = dlopen("libcascci.so", RTLD_LAZY);
    if (libcascci) {
        cci_get_last_insert_id_fp = dlsym(libcascci, "cci_get_last_insert_id");
    }
#endif

}

void uninitCubrid() {

#ifdef WIN32
	FreeLibrary(libcascci);
#else
	dlclose(libcascci);
#endif

    cci_get_last_insert_id_fp = NULL;
    libcascci = NULL;
}

/**
 * Implementation of the Connection/Delegate interface for cubrid. 
 *
 *
 * @file
 */

/* ----------------------------------------------------- Protected methods */

#ifdef PACKAGE_PROTECTED
#pragma GCC visibility push(hidden)
#endif

T CubridConnection_new(URL_T url, char **error) {
	T C;
	T_CCI_ERROR cci_error;
	cubrid_db_conn_t con = 0;

#ifdef WIN32
	cci_init();
#endif

	if (libcascci == NULL) {
        initCubrid();
	}
     
    assert(url);
    assert(error);
    con = doConnect(url, error);
    if (con < 0) {
    	return NULL;
    }

    NEW(C);
    C->db = con;
    C->url = url;
    C->sb = StringBuffer_create(STRLEN);

    cci_set_autocommit(con, CCI_AUTOCOMMIT_TRUE);
	cci_set_lock_timeout(con, 100, &cci_error);
	cci_set_isolation_level(con, TRAN_REP_CLASS_COMMIT_INSTANCE, &cci_error);
    CubridConnection_setQueryTimeout(C, SQL_DEFAULT_TIMEOUT);

    return C;
}

void CubridConnection_free(T *C) {
	T_CCI_ERROR cci_error;

    assert(C && *C);
    
    cci_disconnect((*C)->db, &cci_error);
    if (cci_error.err_code != CCI_ER_NO_ERROR) {
    	DEBUG("disconnect failed %d %s\n", cci_error.err_code, cci_error.err_msg);
    }
    	
    StringBuffer_free(&(*C)->sb);
	FREE(*C);
}

void CubridConnection_setQueryTimeout(T C, int ms) {
	assert(C);

	cci_set_query_timeout(C->db, ms);
	C->timeout = ms;
}

void CubridConnection_setMaxRows(T C, int max) {
	assert(C);

	C->maxRows = max;
}

int CubridConnection_ping(T C) {
    assert(C);

    T_CCI_ERROR error;
    cubrid_db_req_t req_handle;
    char *query = "SELECT 1+1 FROM db_root";
    int rowCount = 0;

    if ((req_handle = cci_prepare(C->db, query, 0, &error)) < 0) {
        SET_LAST_ERROR(C, error);
        return false;
    }

    if ((rowCount = cci_execute(req_handle, 0, 0, &error)) < 0) {
        SET_LAST_ERROR(C, error);
        return false;
    }

    cci_close_req_handle(req_handle);
    req_handle = -1;

    return (C->lastError == CCI_ER_NO_ERROR);
}

int CubridConnection_beginTransaction(T C) {
	assert(C);
    return (C->lastError == CCI_ER_NO_ERROR);
}

int CubridConnection_commit(T C) {
	T_CCI_ERROR	error;
	int res = CCI_ER_NO_ERROR;

    res = cci_end_tran(C->db, CCI_TRAN_COMMIT, &error);
	if (res != CCI_ER_NO_ERROR) {
		SET_LAST_ERROR(C, error);
    }

    return (C->lastError == CCI_ER_NO_ERROR);
}

int CubridConnection_rollback(T C) {

    T_CCI_ERROR	error;
	int res = CCI_ER_NO_ERROR;

	res = cci_end_tran(C->db, CCI_TRAN_ROLLBACK, &error);
	if (res != CCI_ER_NO_ERROR) {
		SET_LAST_ERROR(C, error);
	}

	return (C->lastError == CCI_ER_NO_ERROR);
}


long long int CubridConnection_lastRowId(T C) {
    int res     = 0;
    int retVal  = 0;
    char *value = NULL;
	T_CCI_ERROR error;

	if (cci_get_last_insert_id_fp == NULL) {
		DEBUG("Can't get cci_get_last_insert_id function pointer");
		return 0;
	}

	res = cci_get_last_insert_id_fp(C->db, &value, &error);
	if (res != CCI_ER_NO_ERROR) {
		SET_LAST_ERROR(C, error);
        return 0;
	}

	retVal = atoi(value);
	return (long long)retVal;
}

long long int CubridConnection_rowsChanged(T C) {
    assert(C);

    int res = 0;
    int count = 0;
    T_CCI_ERROR error;

    // memory leak on cci_row_count, WTF!
    res = cci_row_count(C->db, &count, &error);
    if (res != CCI_ER_NO_ERROR) {
        SET_LAST_ERROR(C, error);
        THROW(SQLException, "%s", error.err_msg);
        return 0;
    }

    return count;
}

int CubridConnection_execute(T C, const char *sql, va_list ap) {
    va_list ap_copy;
    T_CCI_ERROR error;
    cubrid_db_req_t req_handle;
    int rowCount = 0;

	assert(C);

    StringBuffer_clear(C->sb);
    va_copy(ap_copy, ap);
    StringBuffer_vappend(C->sb, sql, ap_copy);
    va_end(ap_copy);

    C->lastError = CCI_ER_NO_ERROR;

    //DEBUG("CubridConnection_execute: %s\n", StringBuffer_toString(C->sb));

    if ((req_handle = cci_prepare(C->db, (char *)StringBuffer_toString(C->sb), 0, &error)) < 0) {
        SET_LAST_ERROR(C, error);
        return false;
    }

    if ((rowCount = cci_execute(req_handle, 0, 0, &error)) < 0) {
        SET_LAST_ERROR(C, error);
        CubridConnection_rollback(C);
        return false;
    }

    cci_close_req_handle(req_handle);
    req_handle = -1;
        
    return (C->lastError == CCI_ER_NO_ERROR);
}


ResultSet_T CubridConnection_executeQuery(T C, const char *sql, va_list ap) {
	va_list ap_copy;
    cubrid_db_req_t req;
	T_CCI_ERROR	error;
    int rowCount = 0;
	
	assert(C);

    StringBuffer_clear(C->sb);
    va_copy(ap_copy, ap);
    StringBuffer_vappend(C->sb, sql, ap_copy);
    va_end(ap_copy);
    
    C->lastError = CCI_ER_NO_ERROR;

    //DEBUG("CubridConnection_executeQuery: %s\n", StringBuffer_toString(C->sb));

    if ((req = cci_prepare(C->db, (char *)StringBuffer_toString(C->sb), 0, &error)) < 0) {
        SET_LAST_ERROR(C, error);
		return NULL;
	}

    if ((rowCount = cci_execute(req, 0, 0, &error)) < 0) {
        SET_LAST_ERROR(C, error);
		return NULL;
    }

    return ResultSet_new(CubridResultSet_new(C->db, req, C->maxRows, 0), (Rop_T)&cubridrops);
}

PreparedStatement_T CubridConnection_prepareStatement(T C, const char *sql, va_list ap) {
	va_list ap_copy;
    cubrid_db_req_t req;
	T_CCI_ERROR	error;
    
    assert(C);
    
    StringBuffer_clear(C->sb);
    va_copy(ap_copy, ap);
    StringBuffer_vappend(C->sb, sql, ap_copy);
    va_end(ap_copy);

    C->lastError = CUBRID_OK;

    if ((req = cci_prepare(C->db, (char *)StringBuffer_toString(C->sb), 0, &error)) < 0) {
        SET_LAST_ERROR(C, error);
		return NULL;
	}

 	return PreparedStatement_new(CubridPreparedStatement_new(C->db, req, C->maxRows), (Pop_T)&cubridpops);
}

const char *CubridConnection_getLastError(T C) {
	assert(C);
    
    if (C->lastError != CCI_ER_NO_ERROR && C->sb) {
    	return StringBuffer_toString(C->sb); 
    } else {
    	return "";
    }
}

/* Class method: Cubrid client library finalization */
void CubridConnection_onstop(void) {
	if (libcascci != NULL) {
        uninitCubrid();
	}

#ifdef WIN32
	cci_end();
#endif
}

#ifdef PACKAGE_PROTECTED
#pragma GCC visibility pop
#endif
