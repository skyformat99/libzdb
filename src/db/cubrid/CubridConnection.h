#ifndef CUBRIDCONNECTION_INCLUDED
#define CUBRIDCONNECTION_INCLUDED
#define T ConnectionDelegate_T

T CubridConnection_new(URL_T url, char **error);
void CubridConnection_free(T *C);
void CubridConnection_free(T *C);
void CubridConnection_setQueryTimeout(T C, int ms);
void CubridConnection_setMaxRows(T C, int max);
int CubridConnection_ping(T C);
int CubridConnection_beginTransaction(T C);
int CubridConnection_commit(T C);
int CubridConnection_rollback(T C);
long long int CubridConnection_lastRowId(T C);
long long int CubridConnection_rowsChanged(T C);
int CubridConnection_execute(T C, const char *sql, va_list ap);
ResultSet_T CubridConnection_executeQuery(T C, const char *sql, va_list ap);
PreparedStatement_T CubridConnection_prepareStatement(T C, const char *sql, va_list ap);
const char *CubridConnection_getLastError(T C);
/* Event handlers */
void CubridConnection_onstop(void);
#undef T
#endif