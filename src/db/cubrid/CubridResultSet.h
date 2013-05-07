#ifndef CUBRIDRESULTSET_INCLUDED
#define CUBRIDRESULTSET_INCLUDED
#define T ResultSetDelegate_T
T CubridResultSet_new(int conn, int req, int maxRows);
void CubridResultSet_free(T *R);
int CubridResultSet_getColumnCount(T R);
const char *CubridResultSet_getColumnName(T R, int column);
int CubridResultSet_next(T R);
long CubridResultSet_getColumnSize(T R, int columnIndex);
const char *CubridResultSet_getString(T R, int columnIndex);
const void *CubridResultSet_getBlob(T R, int columnIndex, int *size);
#undef T
#endif
