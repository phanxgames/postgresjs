"use strict";

/**
 * @class
 * <p>For use with Postgresql databases.</p>
 * <p>Created by <a href="http://www.github.com/phanxgames">Phanxgames</a></p>
 *
 * <h4>Requirements:</h4>
 * - ECMAScript 2015 (ES6)<br>
 * - Node.JS 6.2.2 or later (tested on 6.2.2)
 *
 * <h4>Installation:</h4>
 * <pre>
 *      npm install postgresjs
 * </pre>
 * <p>Note: the dictionaryjs, pg and suspend modules should be automatically installed as dependencies, but you may need to install these as well.</p>
 *
 * Copy the dbConfig.ex.json file into your project source folder, rename to dbConfig.json,
 * and update with your database connection information.
 *
 *
 * <h4>Important:</h4>
 * <p>You may only use one connection with each instance of this class.
 * Close any opened connection before opening a new one.
 * You may reuse an instance with a new connection if you close the previous.</p>
 *
 * <h4>Asynchronous nature:</h4>
 * <p>All methods that include a callback (cb) have been designed to be used with the
 * suspend library and may be placed behind a yield command. Be sure to leave the cb
 * parameter null to use the suspend.resume functionality automatically.</p>
 *
 * <p>IMPORTANT: You will also need to set the resume reference in the constructor or the
 * {@link Postgresjs#setResume} method, before the suspend.resume functionality will
 * be enabled.</p>
 *
 * <p>If you do provide a callback, the 3rd parameter, "next" (ex: cb(err,result, next))
 * will be the suspend.resume function reference so you may resume execution
 * to move past the next yield command.</p>
 *
 * <h4>Example:</h4>
 * <pre>
 *   var suspend = require("suspend");
 *   var Postgresjs = require("postgresjs");
 *
 *   //Attach your dbConfig to the Postgresjs module
 *   Postgresjs.config = require('./dbConfig.json');
 *
 *   var db = new Postgresjs(suspend.resume);
 *
 *   suspend(function*() {
 *      yield db.start();
 *
 *      //find all users' email with a username that contains "phanx"
 *      yield db.query("select email from users where username LIKE ? ;",["%phanx%"]);
 *
 *      if (db.error()) {
 *          console.error("Error:",db.error());
 *          return;
 *      }
 *
 *      console.log(db.rowCount + " rows found!");
 *
 *      for (let row of db.rows) {
 *          console.log(row);
 *          if (row.email == "test@test.com") {
 *              //..
 *          }
 *      }
 *
 *      //..place other below examples here..
 *
 *      yield db.end();
 *
 *   })();
 * </pre>
 */
class Postgresjs {

    /**
     * Pass in a unique config just for this instance by using require to get
     * the JSON contents.
     * <h4>Example:</h4>
     * <pre>
     *     //..
     *     var suspend = require("suspend");
     *
     *     var myconfig = require("./dbConfig2.json");
     *     var db = new Postgresjs(suspend.resume,myconfig);
     *     //..
     * </pre>
     * @param {Function} [resume=null] -
     *          set to suspend.resume from generator context
     *          to use suspend library with yields. See {@link setResume}.
     * @param {Object} [config=null] - Overwrite the global config for just this instance.
     */
    constructor(resume,config) {

        this.config = null;

        this.resume = resume;
        this.resume_next = null;

        this.start_stack = null;
        this.opened = null;
        this.client = null;
        this.guid = null;
        this.fnDone = null;

        this.result = null;
        this.resultCount = 0;
        this.last_error = null;

        this.setLocalConfig(config);

    }

    //##########################################################################
    //  Config Methods
    //##########################################################################

    /**
     * @description Required to set this before you first initalize this class.<br>
     * This should be static across your entire project and only needs to be set once.<br>
     * Set to the Contents of dbConfig.json as Object.
     * <h4>Example:</h4>
     * <pre>
     *     var Postgresjs = require("postgresjs");
     *     Postgresjs.config = require("./dbConfig.json");
     *     //..
     * </pre>
     */
    static set config(config) {
        dbConfig.config = config;
    }

    /**
     *
     * @param {Object} config
     */
    setLocalConfig(config) {
        this.config = config;

        if (this.config!=null) {
            updateAutoCloseInterval(config.auto_closer_enabled,config.auto_closer_minutes);
        }
    }

    /**
     * Enables automatic calling of suspend.resume on all methods with
     * optional callbacks.  See Class comment at top of file.
     * @param {Function} resume
     *      set to suspend.resume, set to null to disable
     */
    setResume(resume) {
        this.resume = resume;
    }

    //##########################################################################
    //  Connection Methods
    //##########################################################################

    /**
     * @description Returns connection string with dbConfig values inserted inline.
     */
    get connectionString() {
        let config = dbConfig.config;
        if (this.config)
            config = this.config;

        return "postgres://"+config.username+":"+config.password+"@" +
            config.host+"/"+config.database;
    }

    /**
     * Opens a database connection. Must be called before using any other method.
     * <p>Alias: open(cb)</p>
     * @param {Postgresjs~cbOnError} [cb=null] - Use callback or leave null to use suspend.resume.
     *                                 <br> Returns cb()
     */
    start(cb) {
        var self = this;
        self.initHandleCallback();

        if (self.opened!=null) {
            var err = new Error("Database connection already open.");
            self.handleCallback(cb, err);
            return;
        }

        //get the stack from the scope of method call
        this.start_stack = new Error().stack;

        try {

            pg.connect(self.connectionString, function (err, client, done) {
                if (err) {
                    console.error("Problem getting database connection:\n" + self.start_stack + "\n", err);
                    self.handleCallback(cb,err);
                    self.start_stack = null;
                    return;
                }

                self.guid = generateToken(6, dictTokens);
                self.opened = getTimestamp();
                self.client = client;
                self.done = done;

                openConnections.set(self.guid, self);

                self.handleCallback(cb, null);

            });


        } catch (err) {
            console.error("Problem getting database connection:\n" + self.start_stack + "\n", err);
            self.handleCallback(cb, err);
            self.start_stack = null;
        }

    }

    /**
     * @ignore
     */
    open(cb) {
        this.start(cb);
    }

    /**
     * Closes the connection.
     * You must call this when you are done with this database connection.
     * If you do not end the connection the pool will be exhausted.
     * <p>Alias: close(cb)</p>
     * @param {Postgresjs~cbOnEmpty} [cb=null] - Use callback or leave null to use suspend.resume.
     *                            <br>Returns cb(err)
     */
    end(cb) {
        var self = this;
        self.initHandleCallback();

        if (self.opened!=null) {
            var elapsed = getTimeDiff(self.opened,"ms");
            console.log("Connection released after in use for " + elapsed + " ms.");
        }

        if (self.fnDone!=null)
            self.fnDone();

        openConnections.remove(self.guid);

        self.client = null;
        self.opened = null;
        self.start_stack = null;
        self.result = null;
        self.fnDone = null;

        self.handleCallback(cb, null);

        self.resume = null;
        self.resume_next = null;
    }

    /**
     * @ignore
     */
    close(cb) {
        this.end(cb);
    }

    //##########################################################################
    //  Query Methods
    //##########################################################################

    /**
     * Executes SQL statement on database.
     * <p>Use question-marks (ie: ?) as unnamed parameters within the SQL statement.</p>
     * <h4>Example:</h4>
     * <pre>
     *     //..
     *     yield db.query("select username from users where email=?;",["test@test.com"]);
     *     for (let row of db.rows) {
     *         console.log(row.username);
     *     }
     *     //..
     * </pre>
     * @param {string} sql - sql statement to execute
     * @param {Array} [paras=null] - array of parameters, replacing "?" in SQL
     * @param {Postgresjs~cbOnQuery} [cb=null] - Use callback or leave null to use suspend.resume.
     *              <br>Returns cb(err,results) where result is an array of rows.
     */
    query(sql, paras, cb) {
        var self = this;
        self.initHandleCallback();

        self.resultCount = 0;

        //Check if database connection is open.
        if (self.client==null) {
            var err = new Error("Database Connection is not open.");
            self.handleCallback(cb,err);
            return;
        }

        //get the stack from the scope of method call
        var stack = new Error().stack;

        //Replaces question marks within SQL statement with numbered parameters flags
        sql = replaceQMarks(sql);

        //Start timer to collect query execution time
        var querystart = timeStart();

        //execute the query on the pg client
        self.client.query(sql,paras,function(err,result) {
            //calculate execution time
            var elapsed = timeEnd(querystart);

            //check if there is a problem with the result
            if (err || result==null || !result.hasOwnProperty("rows") ||
                !result.hasOwnProperty("rowCount")) {
                    var errObj = {
                        stack: stack,
                        sql: sql,
                        paras: paras
                    };

                    if (err!=null && err.hasOwnProperty("message"))
                        errObj.message = err.message;
                    else
                        errObj.message = "Unspecified Database Query Error."

                    console.error("Database Error (" + elapsed + "s): ", errObj);
                    self.handleCallback(cb,errObj);
                    return;
            }

            console.log("Query completed in " + elapsed +" seconds.");
            self.result = result.rows;
            self.resultCount = result.rowCount;
            self.handleCallback(cb,null,self.result);

            stack = null;

        });

    }


    /**
     * Merge allows you to insert or update a record by proving insert
     * and update sql statements.
     * <p>Try using instead the {@link Postgresjs#mergeHelper} to generate the
     * two sql statements.</p>
     * @param {String} sqlInsert - sql statement with unnamed parameters (ie: ?)
     * @param {Array} [parasInsert=null] - array of parameters to replace ? in sql
     * @param {String} sqlUpdate - sql statement with unnamed parameters (ie: ?)
     * @param {Array} [parasUpdate=null] - array of parameters to replace ? in sql
     * @param {Postgresjs~cbOnMerge} [cb=null] - Use callback or leave null to use suspend.resume.
     *              <br>Returns cb(err,result) where result will be either
     *              "insert" or "update" depending on which action was used.
     */
    merge(sqlInsert,parasInsert,sqlUpdate,parasUpdate,cb) {
        var self = this;
        self.initHandleCallback();

        self.resultCount = 0;

        if (self.client==null) {
            var err = new Error("Database connect is not open.");
            self.handleCallback(cb,err);
            return;
        }

        var stack = new Error().stack;

        sqlInsert = replaceQMarks(sqlInsert);
        sqlUpdate = replaceQMarks(sqlUpdate);

        var querystart = process.hrtime();

        var loopCount = 0;
        function doLoop() {
            loopCount++;
            if (loopCount > 10) {
                var errObj = {
                    message: "Database Merge exceeded iteration limit",
                    stack: stack,
                    sqlInsert: sqlInsert,
                    sqlUpdate: sqlUpdate,
                    parasInsert: parasInsert,
                    parasUpdate: parasUpdate
                };

                var elapsed = timeEnd(querystart);

                console.error("Database Error ("+ elapsed + " s): ", errObj);

                self.handleCallback(cb,errObj);

                return;
            }

            //attempt to update the record
            self.client.query(sqlUpdate,parasUpdate,function(err,result) {
                var elapsed = timeEnd(querystart);

                if (err) {
                    var errObj = {
                        stack: stack,
                        sql: sqlUpdate,
                        paras: parasUpdate
                    };
                    if (err!=null && err.hasOwnProperty("message"))
                        errObj.message = err.message;
                    else
                        errObj.message = "Unspecified Merge:Update error.";

                    console.error("Database Error (" + elapsed + " s): ",errObj);
                    self.handleCallback(cb,errObj);

                    return;
                }

                //check if any rows were updated/affected
                if (result!=null && result.hasOwnProperty("rowCount") &&
                    result.rowCount > 0) {

                    self.resultCount = result.rowCount;

                    //Merge completed by successfully updating!
                    console.log("Query completed in " + elapsed + " seconds.");

                    self.handleCallback(cb,null,"update");

                    stack = null;

                    return;
                }

                self.client.query(sqlInsert,parasInsert,function(err,result) {
                    var elapsed = timeEnd(querystart);
                    if (err) {
                        //error with inserting? let's try that one again
                        doLoop();
                        return;
                    }

                    if (result!=null && result.hasOwnProperty("rowCount") &&
                        result.rowCount > 0) {

                        console.log("Query completed in " + elapsed + " seconds.");

                        self.resultCount = result.rowCount;

                        self.handleCallback(cb,null,"insert");

                        stack = null;

                        return;
                    }

                    //we shouldn't get to this point, so... Let's loop again!
                    doLoop();

                });
                
            });

        }
        doLoop();
    }




    //##########################################################################
    //  Transaction Methods
    //##########################################################################

    /**
     * Begins a transaction.
     * @param {Postgresjs~cbOnError} [cb=null] - Use callback or leave null to use suspend.resume.
     *              <br>Returns cb(err)
     */
    begin(cb) {
        this.query("START TRANSACTION;",null,cb);
    }

    /**
     * Commits the transaction.
     * @param {Postgresjs~cbOnError} [cb=null] - Use callback or leave null to use suspend.resume.
     *              <br>Returns cb(err)
     */
    commit(cb) {
        this.query("COMMIT;",null,cb);
    }

    /**
     * Rolls back the transaction.
     * @param {Postgresjs~cbOnError} [cb=null] - Use callback or leave null to use suspend.resume.
     *              <br>Returns cb(err)
     */
    rollback(cb) {
        this.query("ROLLBACK;",null,cb);
    };

    //##########################################################################
    //  Helper Methods
    //##########################################################################

    /**
     * Select Statement Helper.
     * <h4>Example:</h4>
     * Select emails from users table that have a username that starts with the letter "h":
     * <pre>
     *     //..
     *     yield db.selectHelper({
     *          table:"users",
     *          columns:["username","email"],
     *          where: db.whereHelper({
     *              "username -like":"h%"
     *          }),
     *          orderBy: db.orderByHelper({
     *              "email":"ASC"
     *          })
     *     });
     *
     *     for (let row of db.rows) {
     *          console.log(row);
     *          //Output example: {username:"Tester",email:"test@test.com"}
     *     }
     *
     *     //..
     * </pre>
     * @param {Object} options - Required. See properties:
     * @param {String} options.table - Table name
     * @param {Array} options.columns - Array of column names to select.
     * @param {String} [options.where=null] - where clause sql statement segment (ie: name=? OR id=? )
     *                      <br>You may also use {@link Postgresjs#whereHelper}
     * @param {Array} [options.whereParas=null] - Array of values to replace parameters in where SQL
     *                      <br>Not needed if you use whereHelper.
     * @param {String} [options.orderBy=null] - order clause sql statement segment (ie: name ASC)
     *                      <br>You may also use {@link Postgresjs#orderByHelper}
     * @param {int} [options.limit=null] - Number of records to return, or null for infinite.
     * @param {int} [options.start=0] - Start row index position.
     * @param {Postgresjs~cbOnQuery} [cb=null] - Use callback or leave null to use suspend.resume.
     *              <br>Returns cb(err,results) where result is an array of rows.
     */
    selectHelper(options,cb) {

        if (options==null) throw Error("SelectHelper: Options parameter is required.");

        var table = options.table || null;
        var columns = options.columns || null;
        var where = options.where || null;
        var whereParas = options.whereParas || null;
        var orderBy = options.orderBy || null;
        var limit = options.limit || null;
        var start = options.start || 0;

        if (table == null) throw Error("SelectHelper: Table option is required.");

        //table,columns,where,whereParas,orderBy

        var sql = "SELECT ";
        var finalParas = null;

        if (Array.isArray(columns))
            sql += columns.join(",");
        else if (columns!=null)
            sql += columns;
        else
            sql += " * ";

        sql += " FROM " + table;
        if (where!=null) {

            if (isObject(where) &&
                where.hasOwnProperty("sql") &&
                where.hasOwnProperty("paras")
            ) {

                sql += " WHERE " + where.sql;
                finalParas = where.paras;

            } else {
                where = where.trim();
                var lowerWhere =  where.toLowerCase();
                if (lowerWhere.substr(0,5)=="where")
                    where.substr(5);

                sql += " WHERE " + where;
                finalParas = whereParas;
            }

        }
        if (orderBy!=null) {
            orderBy = orderBy.trim();
            var lowerOrderBy = orderBy.toLowerCase();
            if (lowerOrderBy.substr(0,8)=="order by")
                orderBy.substr(8);

            sql += " ORDER BY " + orderBy;
        }

        if (limit!=null) {
            sql += " LIMIT " + limit + " OFFSET " + start ;
        }

        sql += " ;";

        if (finalParas==null || finalParas.length==0) finalParas = null;

        this.query(sql,finalParas,cb);

    }

    /**
     * Insert Statement Helper
     * <h4>Example:</h4>
     * Insert new user into users table.
     * <pre>
     *     //..
     *     yield db.insertHelper({
     *          table:"users",
     *          columns:{
     *              "username":"tester",
     *              "email":"oldemail@test.com"
     *          }
     *     });
     *     //..
     * </pre>
     * @param {Object} options - Required. See properties:
     * @param {String} options.table - Table name
     * @param {Array} [options.columns=null] - Leave null to use table order of columns.
     *                  <br>Array of column names to update, use with options.values property to specifiy values.
     * @param {Object} [options.columns=null] - Or Object where keys are column names and values are values.
     *                   Note: options.values property not needed.
     * @param {Array} [options.values=null] - Array of values used with options.columns (Array) property.
     * @param {Postgresjs~cbOnError} [cb=null] - Use callback or leave null to use suspend.resume.
     *                  <br>Returns cb(err)
     */
    insertHelper(options,cb) {

        if (options==null) throw Error("InsertHelper: Options parameter required.");

        var table = options.table || null;
        var columns = options.columns || null;
        var values = options.values || null;

        if (table == null) throw Error("InsertHelper: Table option is required.");

        var sql = "INSERT INTO " + table;


        let tempColumns;
        let tempValues;

        if (columns != null) {

            tempColumns = null;
            tempValues = null;

            if (isObject(columns)) {

                tempColumns = [];
                tempValues = [];

                for (let key in columns) {
                    if (columns.hasOwnProperty(key)) {
                        tempColumns.push(key);
                        tempValues.push(columns[key]);
                    }
                }

            } else {

                //split if columns is a string
                if (!Array.isArray(columns))
                    tempColumns = columns.split(",");
                else
                    tempColumns = columns;

                tempValues = values;
            }

            if (tempValues==null || tempColumns==null ||
                tempColumns.length != tempValues.length) {
                    throw new Error("InsertHelper: Number of Columns and Values do not match.");
                    return;
            }
            sql += "(" + tempColumns.join(",") + ") VALUES ";

        } else {
            tempValues = values;
        }

        sql += " (";

        for (let i=0; i<tempValues.length; i++) {
            sql += "?,";
        }
        sql = removeLastChara(sql) + ");";


        this.query(sql,tempValues,cb);

    }

    /**
     * Update Statement Helper
     * <h4>Example:</h4>
     * Updates user's email by username.
     * <pre>
     *     //..
     *     yield db.updateHelper({
     *          table: "users",
     *          columns: {
     *              "email":"newemail@test.com"
     *          },
     *          where: db.whereHelper({
     *              "username":"tester"
     *          })
     *     });
     *     //..
     * </pre>
     * @param {Object} options - Required. See properties:
     * @param {String} options.table - Table name
     * @param {Array} options.columns - Array of column names to update, use with options.values property to specifiy values.
     * @param {Object} options.columns - Or Object where keys are column names and values are values.
     *                   Note: options.values property not needed.
     * @param {Array} [options.values=null] - Array of values used with options.columns (Array) property.
     * @param {String} [options.where=null] - where clause sql statement segment (ie: name=? OR id=? )
     *                      <br>You may also use {@link Postgresjs#whereHelper}
     * @param {Array} [options.whereParas=null] - Array of values to replace parameters in where SQL
     *                      <br>Not needed if you use whereHelper.
     * @param {Postgresjs~cbOnError} [cb=null] - Use callback or leave null to use suspend.resume.
     *                  <br>Returns cb(err)
     */
    updateHelper(options,cb) {
        var self = this;

        if (options==null) throw Error("UpdateHelper: Options parameter required.");


        var table = options.table;
        var columns = options.columns;
        var values = options.values;
        var where = options.where;
        var whereParas = options.whereParas;

        if (table == null) throw Error("UpdateHelper: Table option is required.");


        var sql = "UPDATE " + table + " SET ";
        var finalParas = null;

        let tempColumns;
        let tempValues;

        if (isObject(columns)) {

            tempColumns = [];
            tempValues = [];

            for (let key in columns) {
                if (columns.hasOwnProperty(key)) {
                    tempColumns.push(key);
                    tempValues.push(columns[key]);
                }
            }

        } else {

            //split if columns is a string
            if (!Array.isArray(columns))
                tempColumns = columns.split(",");
            else
                tempColumns = columns;

            tempValues = values;
        }

        if (tempValues==null || tempColumns==null ||
            tempColumns.length != tempValues.length) {
            throw new Error("UpdateHelper: Number of Columns and Values do not match.");
            return;
        }

        for (let column of tempColumns) {
            sql += column+"=?,";
        }
        sql = removeLastChara(sql);
        finalParas = tempValues;

        if (finalParas==null) finalParas = [];



        if (where!=null) {

            if (isObject(where) &&
                where.hasOwnProperty("sql") &&
                where.hasOwnProperty("paras")
            ) {

                sql += " WHERE " + where.sql;
                Array.prototype.push.apply(finalParas,where.paras);

            } else {
                where = where.trim();
                var lowerWhere =  where.toLowerCase();
                if (lowerWhere.substr(0,5)=="where")
                    where.substr(5);

                sql += " WHERE " + where;
                Array.prototype.push.apply(finalParas,whereParas);

            }

        }

        sql += " ;";

        this.query(sql,finalParas,cb);

    }

    /**
     * Delete Statement Helper
     * <h4>Example:</h4>
     * Deletes user record with the username "tester".
     * <pre>
     *     //..
     *     yield db.deleteHelper({
     *          table: "users",
     *          where: db.whereHelper({
     *              "username":"tester"
     *          })
     *     });
     *     //..
     * </pre>
     * @param {Object} options - Required. See properties:
     * @param {String} options.table - Table name
     * @param {String} [options.where=null] - where clause sql statement segment (ie: name=? OR id=? )
     *                      <br>You may also use {@link Postgresjs#whereHelper}
     * @param {Array} [options.whereParas=null] - Array of values to replace parameters in where SQL
     *                      <br>Not needed if you use whereHelper.
     * @param {int} [options.limit=null] - Number of records to delete, or null for infinite.
     * @param {Postgresjs~cbOnError} [cb=null] - Use callback or leave null to use suspend.resume.
     *                  <br>Returns cb(err)
     */
    deleteHelper(options,cb) {
        var self = this;

        if (options==null) throw Error("DeleteHelper: Options parameter required.");

        var table = options.table;
        var where = options.where;
        var whereParas = options.whereParas;
        var limit = options.limit;

        if (table == null) throw Error("DeleteHelper: Table option is required.");

        var sql = "DELETE FROM " + table + " ";
        var finalParas = null;

        if (where!=null) {

            if (isObject(where) &&
                where.hasOwnProperty("sql") &&
                where.hasOwnProperty("paras")
            ) {

                sql += " WHERE " + where.sql;
                finalParas = where.paras;

            } else {
                where = where.trim();
                var lowerWhere =  where.toLowerCase();
                if (lowerWhere.substr(0,5)=="where")
                    where.substr(5);

                sql += " WHERE " + where;
                finalParas = whereParas;

            }

        }

        if (limit!=null) {
            sql += " LIMIT " + limit;
        }

        sql += " ;";

        this.query(sql,finalParas,cb);
    }

    /**
     * Helper function of Merge method.
     * <p>Merge is a smart method that uses insert or update internally.
     * See {@link Postgresjs#merge} for more information.</p>
     * <h4>Example:</h4>
     * Inserts a new user in the users table or updates their email if already found.
     * <pre>
     *     //..
     *     yield db.mergeHelper({
     *          table: "users",
     *          columns: {
     *              "username":"tester",
     *              "email":"test@test.com"
     *          },
     *          where: db.whereHelper({
     *              "username":"tester"
     *          })
     *     });
     *     //..
     * </pre>
     * @param {Object} options - Required. See properties:
     * @param {String} options.table - Table name
     * @param {Array} options.columns - Array of column names to update, use with options.values property to specifiy values.
     * @param {Object} options.columns - Or Object where keys are column names and values are values.
     *                   Note: options.values property not needed.
     * @param {Array} [options.values=null] - Array of values used with options.columns (Array) property.
     * @param {String} [options.where=null] - where clause sql statement segment (ie: name=? OR id=? )
     *                      <br>You may also use {@link Postgresjs#whereHelper}
     * @param {Array} [options.whereParas=null] - Array of values to replace parameters in where SQL
     *                      <br>Not needed if you use whereHelper.
     * @param {Postgresjs~cbOnMerge} [cb=null] - Use callback or leave null to use suspend.resume.
     *                  <br>Returns cb(err,result) where result is either "update" or "insert" depending on which operation was needed.
     */
    mergeHelper(options,cb) {
        var self = this;

        if (options==null) throw Error("MergeHelper: Options parameter required.");

        var table = options.table;
        var columns = options.columns;
        var values = options.values;
        var where = options.where;
        var whereParas = options.whereParas;

        if (table == null) throw Error("MergeHelper: Table option is required.");


        var sqlInsert = "insert into " + table + " (";
        var sqlUpdate = "update " + table + " set ";
        var parasInsert = [];
        var parasUpdate = [];


        let tempColumns;
        let tempValues;

        if (isObject(columns)) {

            tempColumns = [];
            tempValues = [];

            for (let key in columns) {
                if (columns.hasOwnProperty(key)) {
                    tempColumns.push(key);
                    tempValues.push(columns[key]);
                }
            }

        } else {

            //split if columns is a string
            if (!Array.isArray(columns))
                tempColumns = columns.split(",");
            else
                tempColumns = columns;

            tempValues = values;
        }

        if (tempValues==null || tempColumns==null ||
            tempColumns.length != tempValues.length) {
            throw new Error("MergeHelper: Number of Columns and Values do not match.");
            return;
        }

        for (let column of tempColumns) {
            sqlInsert += column + ",";
            sqlUpdate += column + "=?,";
        }

        sqlInsert = removeLastChara(sqlInsert);
        sqlUpdate = removeLastChara(sqlUpdate);


        //wrapping up the end of the INSERT satement
        sqlInsert += ") VALUES (";
        for (let i=0; i<tempValues.length; i++) {
            sqlInsert += "?,";
        }

        sqlInsert = removeLastChara(sqlInsert);
        sqlInsert += ") ;";

        Array.prototype.push.apply(parasInsert,tempValues);
        Array.prototype.push.apply(parasUpdate,tempValues);


        if (where!=null) {

            if (isObject(where) &&
                where.hasOwnProperty("sql") &&
                where.hasOwnProperty("paras")
            ) {

                sqlUpdate += " WHERE " + where.sql;
                Array.prototype.push.apply(parasUpdate,where.paras);

            } else {
                where = where.trim();
                var lowerWhere =  where.toLowerCase();
                if (lowerWhere.substr(0,5)=="where")
                    where.substr(5);

                sqlUpdate += " WHERE " + where;
                Array.prototype.push.apply(parasUpdate,updateWhereParas);

            }

        }

        self.merge(sqlInsert,parasInsert,sqlUpdate,parasUpdate,cb);

    }


    /**
     * Builds the where option for use with "where" option in other helpers.
     * See {@link Postgresjs#selectHelper}, {@link Postgresjs#deleteHelper},
     * {@link Postgresjs#updateHelper}, {@link Postgresjs#mergeHelper}.
     * <h4>Example:</h4>
     * <pre>
     *     //..
     *     db.whereHelper({
     *       "name -like":"%Smith",
     *       "banned":false
     *     });
     *     //..
     * </pre>
     * @param {Object} variables - Set keys as column names and values as desired value.
     *              <br>Special flags may be appended to key string:
     *              <br> -like  : uses the LIKE comparator
     *              <br> -notlike : uses the NOT LIKE comparator
     *              <br> -not : uses the != comparator
     *              <br> <i>default</i> : uses the = comparator
     * @param {String} [defaultLogic="AND"] - logic seperator (ie: OR)
     * @returns object for use with the "where" option in other helpers.
     */
    whereHelper(variables,defaultLogic) {

        if (variables==null || !isObject(variables))
            return;

        if (defaultLogic==null) defaultLogic = " AND ";

        defaultLogic = defaultLogic.trim();

        var out = {
            sql: null,
            paras: []
        }

        var tempSQL = [];

        for (let key in variables) {

            if (variables.hasOwnProperty(key) && variables[key]!=null) {

                var eqOperator = "=";

                var defaultValueInArrayOperator = " OR ";

                //split on space outside of doublequotes
                var nameParts = key.match(/(?:[^\s"]+|"[^"]*")+/g);
                var name = nameParts[0];

                for (let part of nameParts) {
                    part = part.trim().toLowerCase();
                    if (part.substr(0,2)=="--") part = part.substr(2);
                    if (part.substr(0,1)=="-") part = part.substr(1);
                    switch (part) {
                        case "like":
                            eqOperator = " LIKE ";
                            break;
                        case "notlike":
                            eqOperator = " NOT LIKE ";
                            defaultValueInArrayOperator = " AND ";
                            break;
                        case "not":
                            eqOperator = " != ";
                            defaultValueInArrayOperator = " AND ";
                            break;
                    }
                }


                var thisSQL = name + eqOperator + "?";


                var value = variables[key];
                if (Array.isArray(value)) {

                    let arrSQL = [];

                    for (let arrValue of value) {
                        arrSQL.push(thisSQL);
                        out.paras.push(arrValue);
                    }
                    tempSQL.push("(" + arrSQL.join(defaultValueInArrayOperator) + ")");

                } else {
                    tempSQL.push(thisSQL);
                    out.paras.push(value);
                }
            }
        }

        if (tempSQL.length>0) {
            out.sql = tempSQL.join(" "+defaultLogic+" ");
        }

        if (out.sql==null) out = null;

        return out;


    }

    /**
     * Returns a string to be used in the "orderBy" field in other helpers.
     * See {@link Postgresjs#selectHelper}.
     * <p>Sort options are either "ASC" or "DESC".</p>
     * @param {Object} options - Required. See properties:
     * @param {Array} options.columns - Array of column names, or
     *              <br>Array of Objects: [{name:"column name",sort:"ASC"}, ..], or
     *              <br>Array of Arrays: [["column name","ASC"], ..]
     * @param {String} [options.defaultSort="ASC"] - Used if sort is not specified.
     * @returns string of sql (the order by clause)
     */
    orderByHelper(options) {
        if (options!=null && isObject(options)) {

            var defaultSort = options.defaultSort || "ASC";
            var columns = options.columns || [];

            if (Array.isArray(columns)) {

                var sql = "";

                for (let column of columns) {

                    let name = null;
                    let sort = null;

                    if (isObject(column)) {

                        if (column.name) {
                            name = column.name;
                            sort = column.sort || column.sortBy || null;
                        }

                    } else if (Array.isArray(column)) {
                        if (column.length>=1)
                            name = column[0];

                        if (column.length>=2)
                            sort = column[1];

                    } else {
                        name = column;
                    }


                    if (name) {
                        sql += name + " ";

                        if (sort==null)
                            sql += defaultSort;
                        else {

                            if (sort === true)
                                sql += "ASC";
                            else if (sort === false)
                                sql += "DESC";
                            else
                                sql += sort;

                        }


                        sql += ",";
                    }


                }

                sql = removeLastChara(sql);

                console.log(sql);
                return sql;

            } else
                return columns;

        } else
            return options;

    }

    //##########################################################################
    //  Result Methods
    //##########################################################################

    /**
     * Returns an error object from the last executed command on this db conn.
     * @returns Error object or null if no error
     */
    error() {
        return this.last_error;
    };

    /**
     * @description  Returns an array of objects as rows from last query
     */
    get rows() {
        return this.result;
    }

    /**
     * @description Returns number of rows last affected by last query
     */
    get rowCount() {
        return this.resultCount;
    }


    /**
     * Loops over the rows from the last query. Non-blocking.
     * <h4>Example:</h4>
     * <pre>
     *   //..
     *   yield db.asyncForEach((index,item,next) => {
     *      console.log(index + ") ",item);
     *      //..
     *      next();
     *   });
     *  //..
     * </pre>
     * @param {Postgresjs~cbAsyncForEachIterator} cbIterator - Returns cb(index,item,cbNext).
     *              <br>Must call cbNext() to continue.
     * @param {Postgresjs~cbAsyncForEachFinal} [cbFinal=null] - Returns cb().
     * @param {Boolean} [enableCallback=true] - Set to false to disable default
     *                callback handling (used to disable suspend.resume on loop completion).
     */
    asyncForEach(cbIterator, cbFinal, enableCallback) {
        if (enableCallback==null) enableCallback = true;
        var self = this;
        if (enableCallback) self.initHandleCallback();

        var data = this.result;
        var counter = 0;
        var len = data.length;

        var next = function () {
            if (counter < len && data != null) {
                process.nextTick(step);
            } else {
                if (enableCallback)  self.handleCallback(cbFinal);
                return;
            }
        };
        var step = function () {
            if (counter < len && data != null) {
                var key = counter++;
                if (cbIterator(key, data[key], next) == false) {
                    if (enableCallback) self.handleCallback(cbFinal);
                    return;
                }

            } else {
                if (enableCallback) self.handleCallback(cbFinal);
                return;
            }
        };
        step();
    };


    //##########################################################################
    //  Callback Handlers
    //##########################################################################

    /**
     * Internal method. You should not call this directly.
     * ----------------
     * Prepares the handle callback system for suspend.resume usage.
     * @ignore
     */
    initHandleCallback() {
        if (this.resume)
            this.resume_next = this.resume();
        else
            this.resume_next = null;
    };
    /**
     * Internal method. You should not call this directly.
     * ----------------
     * Calls back the callback or if callback is null, executes
     * the suspend.resume to move forward.
     * @ignore
     */
    handleCallback(cb, err, result) {
        if (err) {
            this.last_error = err;
        } else {
            this.last_error = null;
        }
        if (cb)
            cb(err, result, this.resume_next);
        else if (this.resume_next)
            this.resume_next(err, result);
    }
};
module.exports = Postgresjs;

//##########################################################################
//  Private: Util Methods
//##########################################################################

function replaceQMarks(sql) {
    var insideQuote = false;
    var parameterIndex = 1;
    var currentIndex = 0;
    var rv = [];
    for (var i=0, len = sql.length; i < len; i++) {
        var c = sql[i];
        if (insideQuote) {
            if (c == "'") insideQuote = false;
        } else {
            if (c == '?') {
                rv.push(sql.substring(currentIndex, i));
                rv.push('$' + parameterIndex);
                parameterIndex++;
                currentIndex = i + 1;
            } else if (c=="'") insideQuote = true;
        }
    }
    rv.push(sql.substring(currentIndex));
    return rv.join('');
}
function getTimestamp() {
    var now = new Date();
    return formatDateTime(now);
}
function formatDateTime(str) {
    var input = new Date(str);
    return ""+(input.getFullYear())+"-"+pad(input.getMonth()+1,2,'0')+"-"+pad(input.getDate(),2,'0')+" "+pad(input.getHours(),2,'0')+":"+pad(input.getMinutes(),2,'0')+":"+pad(input.getSeconds(),2,'0');
}
function generateToken(length,dict) {
    var found = false;
    var token = "";
    while (!found) {
        token = generateRandomString(length);
        if (dict==null || (dict!=null && !dict.has(token))) {
            found = true;
        }
    }
    return token;
}
function generateRandomString(L){
    var s= '';
    var randomchar=function(){
        var n= Math.floor(Math.random()*62);
        if(n<10) return n; //1-10
        if(n<36) return String.fromCharCode(n+55); //A-Z
        return String.fromCharCode(n+61); //a-z
    }
    while(s.length< L) s+= randomchar();
    return s;
}
function pad(n, width, z) {
    z = z || '0';
    n = n + '';
    return n.length >= width ? n : new Array(width - n.length + 1).join(z) + n;
}

function timeStart() {
    return process.hrtime();
}
function timeEnd(start) {
    return (process.hrtime(start)[1] / 1000000000).toFixed(5);
}

function removeLastChara(str) {
    return str.substr(0,str.length-1);
}

function replaceAll(str,strFind, strWith) {
    var reg = new RegExp(strFind, 'ig');
    return str.replace(reg, strWith);
}
function isObject(val) {
    if (Array.isArray(val)) return false;
    if (val === null) { return false;}
    return ( (typeof val === 'function') || (typeof val === 'object') );
}


function getTimeDiff(ts,unit) {
    var now = getTimestamp();
    if (ts==null || now==null || ts=="" || now=="") return 0;
    var lastDate = new Date(ts);
    var nowDate = new Date(now);

    var diff = (nowDate-lastDate);

    switch (unit) {
        case "milliseconds":
        case "millisecond":
        case "ms":
            return diff;
            break;
        case "sec":
        case "s":
        case "second":
        case "seconds":
            return diff/1000;
            break;
        case "min":
        case "m":
        case "minutes":
        case "minute":
            return diff/1000/60;
            break;
        case "hour":
        case "h":
        case "hours":
            return diff/1000/60/60;
            break;
        case "days":
        case "day":
        case "d":
            return diff/1000/60/60/24;
            break;
    }

    return 0;
}

//##########################################################################
//  Imports
//##########################################################################

var Dictionary = require('dictionaryjs');
var pg = require('pg');
var suspend = require("suspend");

//global variable
var dbConfig = {config:null};
var dictTokens = new Dictionary();
var openConnections = new Dictionary();

var auto_closer_enabled = false;
var auto_closer_minutes = 3;
var auto_closer_interval;

//##########################################################################
// Auto Close
//##########################################################################

function updateAutoCloseInterval(enabled,minutes) {

    auto_closer_enabled = enabled;
    auto_closer_minutes = minutes;

    if (auto_closer_interval!=null)
        clearInterval(auto_closer_interval);

    if (enabled) {
        auto_closer_interval = setInterval(function() {

            let outlog = "";
            let counter = 0;

            openConnections.asyncForEach(function(guid,db,cbNext) {

                counter++;
                if (db.opened!=null) {
                    let minutes = getTimeDiff(db.opened,"min");
                    if (minutes > auto_closer_minutes) {
                        outlog += "\n[" + db.guid + "] Db Opened : " + minutes +
                            " minutes\n" + db.start_stack+"\n";

                        //auto closer
                        db.end();
                    }
                }
                cbNext();

            },function() {
                if (outlog!="") {
                    console.log("----------------------------------------\n" +
                        "**** " + counter + " Database Connections Open ****"+outlog +
                        "\n----------------------------------------");

                } else {
                    console.log("All database connections are closed ("+counter+").");
                }
            });

        },10000);
    }


}


//##########################################################################
// Type Definitions
//##########################################################################
/**
 * Callback returns no parameters.
 * @callback Postgresjs~cbOnEmpty
 */

/**
 * @callback Postgresjs~cbOnError
 * @param {Error} err - Returns an error if there was one, or null.
 */

/**
 * Returns the result of the query as an array of rows. Example:
 * <pre>
 *     [
 *          {id: 1, name: "Bob"},
 *          {id: 2, name: "John"}
 *     ]
 * </pre>
 * @callback Postgresjs~cbOnQuery
 * @param {Error} err - An error if there was one, or null.
 * @param {Array} result - An array of objects, where each object represents a row, or null.
 */

/**
 * Returns the result of the merge.
 * @callback Postgresjs~cbOnMerge
 * @param {Error} err - An error if there was one, or null.
 * @param {String} result - Either "insert" or "update" depending on what was used.
 */
