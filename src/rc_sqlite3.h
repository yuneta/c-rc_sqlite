/****************************************************************************
 *          RC_SQLITE3.H
 *
 *          Resource Driver for Sqlite3
 *
 *          Copyright (c) 2018 Niyamaka.
 *          All Rights Reserved.
 ****************************************************************************/

#ifndef _C_RC_SQLITE3_H
#define _C_RC_SQLITE3_H 1

#include <sqlite3.h>
#include <yuneta.h>

#ifdef __cplusplus
extern "C"{
#endif

/***************************************************************
 *              Prototypes
 ***************************************************************/
PUBLIC dba_persistent_t *dba_rc_sqlite3(void);

#ifdef __cplusplus
}
#endif

#endif
