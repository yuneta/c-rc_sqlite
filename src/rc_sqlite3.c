/***********************************************************************
 *          RC_SQLITE3.C
 *
 *          Resource Driver for Sqlite3
 *
 *          Copyright (c) 2018 Niyamaka.
 *          All Rights Reserved.
***********************************************************************/
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include "rc_sqlite3.h"

/***************************************************************
 *              Constants
 ***************************************************************/

/***************************************************************
 *              Structures
 ***************************************************************/

/***************************************************************
 *              DBA persistent functions
 ***************************************************************/
PRIVATE void * dba_open(
    hgobj gobj,
    const char *database,
    json_t *jn_properties   // owned
);
PRIVATE int dba_close(hgobj gobj, void *pDb);
PRIVATE int dba_create_table(
    hgobj gobj,
    void *pDb,
    const char *tablename,
    const char *key,
    json_t *kw_fields // owned
);
PRIVATE int dba_drop_table(
    hgobj gobj,
    void *pDb,
    const char *tablename
);
PRIVATE uint64_t dba_create_record(
    hgobj gobj,
    void *pDb,
    const char *tablename,
    json_t *kw_record // owned
);

PRIVATE int dba_update_record(
    hgobj gobj,
    void *pDb,
    const char *tablename,
    json_t *kw_filtro,  // owned
    json_t *kw_record   // owned
);

PRIVATE int dba_delete_record(
    hgobj gobj,
    void *pDb,
    const char *tablename,
    json_t *kw_filtro // owned
);

PRIVATE json_t *dba_load_table(
    hgobj gobj,
    void *pDb,
    const char* tablename,
    const char* resource,
    void *user_data,    // To use as parameter in dba_record_cb() callback.
    json_t *kw_filtro,  // owned. Filter the records if you don't full table. See kw_record keys.
    dba_record_cb dba_filter,
    json_t *jn_record_list
);

/***************************************************************
 *              Prototypes
 ***************************************************************/
PRIVATE int one_step(hgobj gobj, const char *sql, sqlite3 *pDb);
PRIVATE GBUFFER *sqlite_create_table(
    hgobj gobj,
    const char *tablename,
    const char *key,
    json_t *kw_fields
);
PRIVATE GBUFFER *sqlite_drop_table(hgobj gobj, const char *tablename);
PRIVATE GBUFFER *sqlite_insert_new(
    hgobj gobj,
    const char *tablename,
    json_t *kw_record
);
PRIVATE GBUFFER *sqlite_update_id(
    hgobj gobj,
    const char *tablename,
    json_int_t id,
    json_t *kw_record
);
PRIVATE GBUFFER *sqlite_delete_id(
    hgobj gobj,
    const char *tablename,
    json_int_t id
);

PRIVATE GBUFFER *sqlite_select(
    hgobj gobj,
    const char *tablename,
    json_t *kw_filtro  // owned
);

PRIVATE json_t *sqlrow2json(
    hgobj gobj,
    sqlite3_stmt *pStmt
);

/***************************************************************
 *              Data
 ***************************************************************/
PRIVATE BOOL __sqlite_initialized__ = FALSE;
PRIVATE BOOL verbose;

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE dba_persistent_t dba = {
    dba_open,
    dba_close,
    dba_create_table,
    dba_drop_table,
    dba_create_record,
    dba_update_record,
    dba_delete_record,
    dba_load_table
};
PUBLIC dba_persistent_t *dba_rc_sqlite3(void)
{
    return &dba;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE void sqlite_errorLogCallback(hgobj gobj, int iErrCode, const char *zMsg)
{
    log_error(0,
        "gobj",         "%s", gobj_full_name(gobj),
        "function",     "%s", __FUNCTION__,
        "msgset",       "%s", MSGSET_SERVICE_ERROR,
        "msg",          "%s", "sqlite error msg",
        "error",        "%d", iErrCode,
        "errormsg",     "%s", zMsg,
        NULL
    );
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE void * dba_open(
    hgobj gobj,
    const char *database,
    json_t *jn_properties   // owned
)
{
    int ret;
    sqlite3 *pDb;

    if(!__sqlite_initialized__) { // Global variable, only one time can be called sqlite3_config()
        __sqlite_initialized__ = TRUE;
        sqlite3_config(SQLITE_CONFIG_LOG, sqlite_errorLogCallback, gobj);
    }
    if(access(database, 0)==0) {
        ret = sqlite3_open_v2(database, &pDb, SQLITE_OPEN_READWRITE , 0);
    } else {
        ret = sqlite3_open_v2(database, &pDb, SQLITE_OPEN_CREATE|SQLITE_OPEN_READWRITE, 0);
        chmod(database, yuneta_rpermission());
    }
    if(ret != SQLITE_OK) {
        log_error(0,
            "gobj",         "%s", gobj_full_name(gobj),
            "function",     "%s", __FUNCTION__,
            "msgset",       "%s", MSGSET_SERVICE_ERROR,
            "msg",          "%s", "sqlite3_open_v2() FAILED",
            "ret",          "%d", ret,
            "error",        "%d", sqlite3_errcode(pDb),
            "errormsg",     "%s", sqlite3_errstr(sqlite3_errcode(pDb)),
            NULL
        );
    }
    if(1) { // !gobj_read_bool_attr(gobj, "disable_fkeys")) {
        one_step(gobj, "PRAGMA foreign_keys = ON;", pDb);
    }
    JSON_DECREF(jn_properties);
    return pDb;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int dba_close(hgobj gobj, void *pDb)
{
    return sqlite3_close(pDb);
}

/***************************************************************************
 *  HACK this function MUST BE idempotent!
 ***************************************************************************/
PRIVATE int dba_create_table(
    hgobj gobj,
    void *pDb,
    const char *tablename,
    const char *key,
    json_t *kw_fields   // owned
)
{
    GBUFFER *gbuf_sql = sqlite_create_table(gobj, tablename, key, kw_fields);
    if(!gbuf_sql) {
        // Error already logged
        KW_DECREF(kw_fields);
        return -1;
    }
    int ret = one_step(gobj, gbuf_cur_rd_pointer(gbuf_sql), pDb);
    gbuf_decref(gbuf_sql);
    KW_DECREF(kw_fields);
    return ret;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int dba_drop_table(
    hgobj gobj,
    void *pDb,
    const char *tablename
)
{
    GBUFFER *gbuf_sql = sqlite_drop_table(gobj, tablename);
    if(!gbuf_sql) {
        // Error already logged
        return -1;
    }
    int ret = one_step(gobj, gbuf_cur_rd_pointer(gbuf_sql), pDb);
    gbuf_decref(gbuf_sql);
    return ret;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE uint64_t dba_create_record(
    hgobj gobj,
    void *pDb,
    const char *tablename,
    json_t *kw_record  // owned
)
{
    json_int_t id = kw_get_int(kw_record, "id", 0, 0);
    if(id==0) {
        /*
         *  Remove the id columns
         *  to let sqlite assign the id if not set by user
         */
        json_object_del(kw_record, "id");
    }
    /*
     *  Insert sqlite sql
     */
    GBUFFER *gbuf_sql = sqlite_insert_new(
        gobj,
        tablename,
        kw_record // owned
    );
    if(!gbuf_sql) {
        // Error already logged
        return -1;
    }

    /*
     *  Ejecuta el script
     */
    int ret = one_step(gobj, gbuf_cur_rd_pointer(gbuf_sql), pDb);
    if(ret < 0) {
        // Error already logged
        gbuf_decref(gbuf_sql);
        return -1;
    }
    gbuf_decref(gbuf_sql);

    /*
     *  Get the id given by sqlite (given by us, or not).
     */
    sqlite3_int64 rowid = sqlite3_last_insert_rowid(pDb);
    return rowid;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int dba_update_record(
    hgobj gobj,
    void *pDb,
    const char *tablename,
    json_t *kw_filtro,  // owned
    json_t *kw_record   // owned
)
{
    uint64_t id = kw_get_int(kw_filtro, "id", 0, KW_REQUIRED);
    KW_DECREF(kw_filtro);
    json_object_del(kw_record, "id");

    /*
     *  Update sqlite sql
     */
    GBUFFER *gbuf_sql;
    gbuf_sql = sqlite_update_id(
        gobj,
        tablename,
        id,
        kw_record // owned
    );

    if(!gbuf_sql) {
        // Error already logged
        return -1;
    }

    /*
     *  Ejecuta el script
     */
    int ret = one_step(gobj, gbuf_cur_rd_pointer(gbuf_sql), pDb);
    gbuf_decref(gbuf_sql);
    return ret;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int dba_delete_record(
    hgobj gobj,
    void *pDb,
    const char *tablename,
    json_t *kw_filtro // owned
)
{
    // By the moment, uso restringido de kw_filtro, solo para id.
    uint64_t id = kw_get_int(kw_filtro, "id", 0, KW_REQUIRED);
    KW_DECREF(kw_filtro);

    GBUFFER *gbuf_sql;
    gbuf_sql = sqlite_delete_id(
        gobj,
        tablename,
        id
    );
    if(!gbuf_sql) {
        return -1;
    }

    /*
     *  Ejecuta el script
     */
    int ret = one_step(gobj, gbuf_cur_rd_pointer(gbuf_sql), pDb);
    if(ret < 0) {
        // Error already logged
        gbuf_decref(gbuf_sql);
        return -1;
    }
    gbuf_decref(gbuf_sql);
    return 0;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE json_t *dba_load_table(
    hgobj gobj,
    void *pDb,
    const char* tablename,
    const char* resource,
    void *user_data,    // To use as parameter in dba_record_cb() callback.
    json_t *kw_filtro,  // owned. Filter the records if you don't full table. See kw_record keys.
    dba_record_cb dba_filter,
    json_t *jn_record_list
)
{
    if(!jn_record_list) {
        jn_record_list = json_array();
    }

    GBUFFER *gbuf_sql = sqlite_select(
        gobj,
        tablename,
        kw_filtro  // owned
    );
    if(!gbuf_sql) {
        // Error already logged
        return jn_record_list;
    }

    sqlite3_stmt *pStmt;
    const char *sql = gbuf_cur_rd_pointer(gbuf_sql);

    int ret = sqlite3_prepare_v2(
        pDb,
        sql,
        -1,
        &pStmt,
        0
    );
    if(ret != SQLITE_OK) {
        log_error(0,
            "gobj",         "%s", gobj_full_name(gobj),
            "function",     "%s", __FUNCTION__,
            "msgset",       "%s", MSGSET_SERVICE_ERROR,
            "msg",          "%s", "sqlite3_prepare_v2() FAILED",
            "sql",          "%s", sql,
            "ret",          "%d", ret,
            "error",        "%d", sqlite3_errcode(pDb),
            "errormsg",     "%s", sqlite3_errstr(sqlite3_errcode(pDb)),
            NULL
        );
        log_debug_dump(
            0,
            sql,
            strlen(sql),
            "sqlite3_prepare_v2() FAILED"
        );
        gbuf_decref(gbuf_sql);
        return jn_record_list;
    }
    while(TRUE) {
        ret = sqlite3_step(pStmt);
        if(ret == SQLITE_ROW) {
            json_t *kw_record = sqlrow2json(gobj, pStmt);
            JSON_INCREF(kw_record);
            int ret = dba_filter(gobj, resource, user_data, kw_record);
            // Return 1 append, 0 ignore, -1 break the load.
            if(ret < 0) {
                JSON_DECREF(kw_record);
                break;
            } else if(ret==0) {
                JSON_DECREF(kw_record);
                continue;
            }
            json_array_append_new(jn_record_list, kw_record);

        } else if(ret == SQLITE_DONE) {
            break;

        } else {
            log_error(0,
                "gobj",         "%s", gobj_full_name(gobj),
                "function",     "%s", __FUNCTION__,
                "msgset",       "%s", MSGSET_SERVICE_ERROR,
                "msg",          "%s", "sqlite3_step() FAILED",
                "ret",          "%d", ret,
                "error",        "%d", sqlite3_errcode(pDb),
                "errormsg",     "%s", sqlite3_errstr(sqlite3_errcode(pDb)),
                NULL
            );
            sqlite3_finalize(pStmt);
            gbuf_decref(gbuf_sql);
            return jn_record_list;
        }
    }
    sqlite3_finalize(pStmt);

    gbuf_decref(gbuf_sql);

    return jn_record_list;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int one_step(hgobj gobj, const char *sql, sqlite3 *pDb)
{
    sqlite3_stmt *pStmt;
    if(verbose) {
        log_info(0,
            "gobj",         "%s", gobj_full_name(gobj),
            "function",     "%s", __FUNCTION__,
            "msgset",       "%s", MSGSET_DATABASE,
            "msg",          "%s", "sql one step",
            "sql",          "%s", sql,
            NULL
        );
    }
    const char *pzTail;
    int ret = sqlite3_prepare_v2(
        pDb,
        sql,
        -1,
        &pStmt,
        &pzTail
    );
    if(ret != SQLITE_OK) {
        log_error(0,
            "gobj",         "%s", gobj_full_name(gobj),
            "function",     "%s", __FUNCTION__,
            "msgset",       "%s", MSGSET_SERVICE_ERROR,
            "msg",          "%s", "sqlite3_prepare_v2() FAILED",
            "sql",          "%s", sql,
            "ret",          "%d", ret,
            "error",        "%d", sqlite3_errcode(pDb),
            "errormsg",     "%s", sqlite3_errstr(sqlite3_errcode(pDb)),
            NULL
        );
        log_debug_dump(
            0,
            sql,
            strlen(sql),
            "sqlite3_prepare_v2() FAILED"
        );
        return -1;
    }

    ret = sqlite3_step(pStmt);
    if(ret != SQLITE_DONE) {
        const char *errmsg = sqlite3_errstr(sqlite3_errcode(pDb));
        sqlite3_finalize(pStmt);
        log_error(0,
            "gobj",         "%s", gobj_full_name(gobj),
            "function",     "%s", __FUNCTION__,
            "msgset",       "%s", MSGSET_SERVICE_ERROR,
            "msg",          "%s", "sqlite3_step() FAILED",
            "error",        "%d", ret,
            "errormsg",     "%s", errmsg?errmsg:"?",
            NULL
        );
        return -1;
    }
    sqlite3_finalize(pStmt);

    return 0;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE const char *jsontype2sqltype(json_t *jn)
{
    if(json_is_string(jn)) {
        return "TEXT";
    } else if(json_is_integer(jn)) {
        return "INTEGER";
    } else if(json_is_real(jn)) {
        return "REAL";
    } else {
        return "BLOB";
    }
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE GBUFFER *sqlite_create_table(
    hgobj gobj,
    const char *tablename,
    const char *key,
    json_t *kw_fields
)
{
    GBUFFER *gbuf_script = gbuf_create(4*1024, gbmem_get_maximum_block(), 0, 0);
    if(!gbuf_script) {
        // Error already logged
        return 0;
    }
    gbuf_printf(gbuf_script, "CREATE TABLE IF NOT EXISTS %s (", tablename);

    int cols = 0;
    const char *k;
    json_t *jn_value;
    json_object_foreach(kw_fields, k, jn_value) {
        const char *type = jsontype2sqltype(jn_value);
        if(!type) {
            log_error(0,
                "gobj",         "%s", gobj_full_name(gobj),
                "function",     "%s", __FUNCTION__,
                "msgset",       "%s", MSGSET_SERVICE_ERROR,
                "msg",          "%s", "jsontype2sqltype() type not valid",
                "tablename",    "%s", tablename,
                "field",        "%s", k,
                NULL
            );
            continue;
        }
        if(cols > 0)  {
            gbuf_printf(gbuf_script, ", ");
        }
        gbuf_printf(gbuf_script, "%s %s", k, type);
        cols++;
    }

    /*
     *  Primary key
     */
    if(key) {
        gbuf_printf(gbuf_script, ", PRIMARY KEY (");
        gbuf_printf(gbuf_script, "%s", key);
        gbuf_printf(gbuf_script, ")");
    }

    gbuf_printf(gbuf_script, ");");

    return gbuf_script;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE GBUFFER *sqlite_drop_table(hgobj gobj, const char *tablename)
{
    GBUFFER *gbuf_script = gbuf_create(1*1024, gbmem_get_maximum_block(), 0, 0);
    if(!gbuf_script) {
        // Error already logged
        return 0;
    }
    gbuf_printf(gbuf_script, "DROP TABLE %s ;", tablename);
    return gbuf_script;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int write_db_value(hgobj gobj, GBUFFER *gbuf_script, json_t *value)
{
    if(json_is_string(value)) {
        const char *s = json_string_value(value);
        char *sq = sqlite3_mprintf("%q", s);
        if(sq) {
            gbuf_printf(gbuf_script, "'%s'", sq);
            sqlite3_free(sq);
        } else {
            log_error(0,
                "gobj",         "%s", gobj_full_name(gobj),
                "function",     "%s", __FUNCTION__,
                "msgset",       "%s", MSGSET_SERVICE_ERROR,
                "msg",          "%s", "sqlite3_mprintf() FAILED",
                "str",          "%s", s,
                NULL
            );
        }
    } else if(json_is_integer(value)) {
        json_int_t d = json_integer_value(value);
        gbuf_printf(gbuf_script, "%" JSON_INTEGER_FORMAT, d);
    } else if(json_is_real(value)) {
        double d = json_real_value(value);
        gbuf_printf(gbuf_script, "%.f", d);
    } else if(json_is_true(value)) {
        gbuf_printf(gbuf_script, "1");
    } else if(json_is_false(value)) {
        gbuf_printf(gbuf_script, "0");
    } else if(json_is_null(value)) {
        gbuf_printf(gbuf_script, "0");
    } else if(json_is_array(value) || json_is_object(value)) {
        char *s = json_dumps(value, JSON_ENCODE_ANY|JSON_COMPACT); //|JSON_SORT_KEYS

        char *sq = sqlite3_mprintf("%q", s);
        if(sq) {
            gbuf_printf(gbuf_script, "'%s'", sq);
            sqlite3_free(sq);
        } else {
            log_error(0,
                "gobj",         "%s", gobj_full_name(gobj),
                "function",     "%s", __FUNCTION__,
                "msgset",       "%s", MSGSET_SERVICE_ERROR,
                "msg",          "%s", "sqlite3_mprintf() FAILED",
                "str",          "%s", s,
                NULL
            );
        }
        gbmem_free(s);

    } else {
        log_error(0,
            "gobj",         "%s", gobj_full_name(gobj),
            "function",     "%s", __FUNCTION__,
            "msgset",       "%s", MSGSET_SERVICE_ERROR,
            "msg",          "%s", "case INVALID",
            NULL
        );
        return -1;
    }
    return 0;
}

/***************************************************************************
 *  WRITE: insert new resource
 ***************************************************************************/
PRIVATE GBUFFER *sqlite_insert_new(
    hgobj gobj,
    const char *tablename,
    json_t *kw_record // owned
)
{
    GBUFFER *gbuf_script = gbuf_create(4*1024, gbmem_get_maximum_block(), 0, 0);
    if(!gbuf_script) {
        // Error already logged
        KW_DECREF(kw_record);
        return 0;
    }
    gbuf_printf(gbuf_script, "INSERT INTO %s (", tablename);

    const char *key;
    json_t *value;
    int i;

    /*
     *  Set COLS
     */
    i = 0;
    json_object_foreach(kw_record, key, value) {
        if(i == 0) {
            gbuf_printf(gbuf_script, "%s", key);
        } else {
            gbuf_printf(gbuf_script, ", %s", key);
        }
        i++;
    }
    gbuf_printf(gbuf_script, ") VALUES (");
    /*
     *  Set VALUES
     */
    i = 0;
    json_object_foreach(kw_record, key, value) {
        if(i > 0) {
            gbuf_printf(gbuf_script, ", ");
        }

        int ret = write_db_value(gobj, gbuf_script, value);
        if(ret < 0) {
            log_error(0,
                "gobj",         "%s", gobj_full_name(gobj),
                "function",     "%s", __FUNCTION__,
                "msgset",       "%s", MSGSET_SERVICE_ERROR,
                "msg",          "%s", "json type INVALID",
                "tablename",    "%s", tablename,
                "key",          "%s", key,
                NULL
            );
        }

        i++;
    }

    gbuf_printf(gbuf_script, ");");

    KW_DECREF(kw_record);

    return gbuf_script;
}

/***************************************************************************
 *  WRITE: update resource
 *  mandatory id
 ***************************************************************************/
PRIVATE GBUFFER *sqlite_update_id(
    hgobj gobj,
    const char *tablename,
    json_int_t id,
    json_t *kw_record // owned
)
{
    GBUFFER *gbuf_script = gbuf_create(4*1024, gbmem_get_maximum_block(), 0, 0);
    if(!gbuf_script) {
        // Error already logged
        KW_DECREF(kw_record);
        return 0;
    }
    gbuf_printf(gbuf_script, "UPDATE %s SET ", tablename);

    const char *key;
    json_t *value;
    int i;

    /*
     *  Set COLS/VALUES
     */
    i = 0;
    json_object_foreach(kw_record, key, value) {
        if(strcasecmp(key, "id")==0) {
            continue;
        }
        if(i > 0) {
            gbuf_printf(gbuf_script, ", ");
        }
        gbuf_printf(gbuf_script, "%s", key);
        gbuf_printf(gbuf_script, " = ");

        int ret = write_db_value(gobj, gbuf_script, value);
        if(ret < 0) {
            log_error(0,
                "gobj",         "%s", gobj_full_name(gobj),
                "function",     "%s", __FUNCTION__,
                "msgset",       "%s", MSGSET_SERVICE_ERROR,
                "msg",          "%s", "json type INVALID",
                "tablename",    "%s", tablename,
                "key",          "%s", key,
                NULL
            );
        }

        i++;
    }

    gbuf_printf(gbuf_script, " WHERE id=%" JSON_INTEGER_FORMAT ";", id);

    KW_DECREF(kw_record);

    return gbuf_script;
}

/***************************************************************************
 *  WRITE: delete resource
 *  mandatory id
 ***************************************************************************/
PRIVATE GBUFFER *sqlite_delete_id(
    hgobj gobj,
    const char *tablename,
    json_int_t id)
{
    GBUFFER *gbuf_script = gbuf_create(4*1024, gbmem_get_maximum_block(), 0, 0);
    if(!gbuf_script) {
        // Error already logged
        return 0;
    }
    gbuf_printf(gbuf_script, "DELETE FROM %s WHERE id=%" JSON_INTEGER_FORMAT ";", tablename, id);
    return gbuf_script;
}

/***************************************************************************
 *  READ: read resources
 ***************************************************************************/
PRIVATE GBUFFER *sqlite_select(
    hgobj gobj,
    const char *tablename,
    json_t *kw_filtro  // owned
)
{
    GBUFFER *gbuf_script = gbuf_create(4*1024, gbmem_get_maximum_block(), 0, 0);
    if(!gbuf_script) {
        // Error already logged
        KW_DECREF(kw_filtro);
        return 0;
    }

    gbuf_printf(gbuf_script, "SELECT * FROM %s ", tablename);

    if(json_object_size(kw_filtro)>0) {
        gbuf_printf(gbuf_script, " WHERE ");
        int cols = 0;
        const char *k;
        json_t *jn_value;
        json_object_foreach(kw_filtro, k, jn_value) {
            if(cols > 0)  {
                gbuf_printf(gbuf_script, " AND ");
            }
            if(json_is_integer(jn_value)) {
                gbuf_printf(gbuf_script, "%s=%"JSON_INTEGER_FORMAT, k, json_integer_value(jn_value));

            } else if(json_is_string(jn_value)) {
                gbuf_printf(gbuf_script, "%s='%s'", k, json_string_value(jn_value));

            } else if(json_is_real(jn_value)) {
                gbuf_printf(gbuf_script, "%s=%f", k, json_real_value(jn_value));

            }
            cols++;
        }
    }
    gbuf_printf(gbuf_script, " ;");

    KW_DECREF(kw_filtro);
    return gbuf_script;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE json_t *sqlrow2json(
    hgobj gobj,
    sqlite3_stmt *pStmt)
{
    json_t *kw_record = json_object();

    int cols = sqlite3_column_count(pStmt);
    for(int i=0; i<cols; i++) {
        const char *key = sqlite3_column_name(pStmt, i);
        const char *type = sqlite3_column_decltype(pStmt, i);
        if(strcasecmp(type, "INTEGER")==0) {
            sqlite3_int64 v_i = sqlite3_column_int64(pStmt, i);
            json_object_set_new(kw_record, key, json_integer((json_int_t)v_i));

        } else if(strcasecmp(type, "REAL")==0) {
            double v_d = sqlite3_column_double(pStmt, i);
            json_object_set_new(kw_record, key, json_real(v_d));

        } else if(strcasecmp(type, "TEXT")==0) {
            const char *v_s = (const char *)sqlite3_column_text(pStmt, i);
            json_object_set_new(kw_record, key, json_string(v_s));

        } else if(strcasecmp(type, "BLOB")==0) {
            int size = sqlite3_column_bytes(pStmt, i);
            const char *v_b = sqlite3_column_blob(pStmt, i);
            if(v_b) {
                json_t *jn_v = nonlegalbuffer2json(v_b, size, TRUE);
                if(jn_v) {
                    json_object_set_new(kw_record, key, jn_v);
                }
            }

        } else {
            log_error(0,
                "gobj",         "%s", gobj_full_name(gobj),
                "function",     "%s", __FUNCTION__,
                "msgset",       "%s", MSGSET_SERVICE_ERROR,
                "msg",          "%s", "sqlite3_column_type() type UNKNOWN",
                "col",          "%s", key,
                "type",         "%s", type,
                NULL
            );
        }
    }

    return kw_record;
}
