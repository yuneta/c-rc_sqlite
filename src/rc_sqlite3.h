/****************************************************************************
 *          RC_SQLITE3.H
 *
 *          Resource Driver for Sqlite3
 *
 *          Copyright (c) 2018 Niyamaka.
 *          All Rights Reserved.
 ****************************************************************************/
#pragma once

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
