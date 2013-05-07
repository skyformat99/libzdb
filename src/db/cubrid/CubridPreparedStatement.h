#ifndef CUBRIDPREPAREDSTATEMENT_INCLUDED
#define CUBRIDPREPAREDSTATEMENT_INCLUDED
#define T PreparedStatementDelegate_T
T CubridPreparedStatement_new(int conn, int req, int maxRows);
void CubridPreparedStatement_free(T *P);
void CubridPreparedStatement_setString(T P, int parameterIndex, const char *x);
void CubridPreparedStatement_setInt(T P, int parameterIndex, int x);
void CubridPreparedStatement_setLLong(T P, int parameterIndex, long long int x);
void CubridPreparedStatement_setDouble(T P, int parameterIndex, double x);
void CubridPreparedStatement_setBlob(T P, int parameterIndex, const void *x, int size);
void CubridPreparedStatement_execute(T P);
ResultSet_T CubridPreparedStatement_executeQuery(T P);
#undef T
#endif