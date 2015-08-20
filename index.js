/**
 * Created by Kyriakos Barbounakis<k.barbounakis@gmail.com> on 26/11/2014.
 *
 * Copyright (c) 2015, Kyriakos Barbounakis k.barbounakis@gmail.com
                       Anthi Oikonomou anthioikonomou@gmail.com
 All rights reserved.
 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this
 list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright notice,
 this list of conditions and the following disclaimer in the documentation
 and/or other materials provided with the distribution.
 * Neither the name of MOST Web Framework nor the names of its
 contributors may be used to endorse or promote products derived from
 this software without specific prior written permission.
 THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
var async = require('async'),
    util = require('util'),
    qry = require('most-query'),
    oracledb = require('oracledb');
/**
 * native extensions
 */
if (typeof Object.isNullOrUndefined !== 'function') {
    /**
     * Gets a boolean that indicates whether the given object is null or undefined
     * @param {*} obj
     * @returns {boolean}
     */
    Object.isNullOrUndefined = function(obj) {
        return (typeof obj === 'undefined') || (obj==null);
    }
}

/**
 * @class OracleAdapter
 * @augments DataAdapter
 * @param {{host:string,port:number,schema:string,user:string,password:string,service:string,type:string,instance:string,connectString:string}|*} options
 * @property {string} connectString
 * @constructor
 */
function OracleAdapter(options) {
    this.options = options || { host:'localhost' };
    /**
     * Represents the database raw connection associated with this adapter
     * @type {*}
     */
    this.rawConnection = null;
    var connectString;
    //of options contains connectString parameter ignore all other params and define this as the database connection string
    if (options.connectString) { connectString = options.connectString; }
    Object.defineProperty(this, 'connectString', {
        get: function() {
            if (typeof connectString === 'string') {
                return connectString;
            }
            else {
                //generate connectString ([//]host_name[:port][/service_name][:server_type][/instance_name])
                //get hostname or localhost
                connectString = options.host || 'localhost';
                //append port
                if (typeof options.port !== 'undefined') { connectString += ':' + options.port; }
                if (typeof options.service !== 'undefined') { connectString += '/' + options.service; }
                if (typeof options.type !== 'undefined') { connectString += ':' + options.type; }
                if (typeof options.instance !== 'undefined') { connectString += '/' + options.instance; }
                return connectString;
            }
        }
    });
}

OracleAdapter.prototype.open = function(callback) {
    var self = this;
    callback = callback || function() {};
    if (self.rawConnection) {
        callback();
    }
    else {
        oracledb.getConnection(
            {
                user          : this.options.user,
                password      : this.options.password,
                connectString : this.connectString
            }, function(err, connection) {
                if (err) { return callback(err); }
                self.rawConnection = connection;
                callback();
            });
    }
};

OracleAdapter.prototype.close = function(callback) {
    var self = this;
    callback = callback || function() {};
    try {
        if (self.rawConnection)
        {
            //close connection
            self.rawConnection.release(function(err) {
                if (process.env.NODE_ENV === 'development') {
                    console.log('An error occured while closing database.');
                    console.log(err.message);
                    if (err.stack) { console.log(err.stack); }
                }
                //and finally return
                callback();
            });
        }
        else {
            callback();
        }

    }
    catch (e) {
        console.log('An error occured while closing database.');
        console.log(e.message);
        //call callback without error
        callback();
    }
};

/**
 * @param {string} query
 * @param {*=} values
 */
OracleAdapter.prototype.prepare = function(query,values) {
    return qry.prepare(query,values)
};

OracleAdapter.formatType = function(field)
{
    var size = parseInt(field.size), s;
    switch (field.type)
    {
        case 'Boolean':
            s = 'NUMBER(1,0)';
            break;
        case 'Byte':
            s = 'NUMBER(1,0)';
            break;
        case 'Number':
        case 'Float':
            s = 'NUMBER';
            break;
        case 'Counter':
            return 'NUMBER(10)';
        case 'Currency':
            s =  'NUMBER(' + (field.size || 19) + ',4)';
            break;
        case 'Decimal':
            s =  'NUMBER';
            if ((field.size) && (field.scale)) { s += '(' + field.size + ',' + field.scale + ')'; }
            break;
        case 'Date':
        case 'DateTime':
            s = 'TIMESTAMP WITH TIME ZONE';
            break;
        case 'Time':
            s = 'NUMBER';
            break;
        case 'Long':
        case 'Duration':
            s = 'NUMBER(10)';
            break;
        case 'Integer':
            s = 'NUMBER' + (field.size ? '(' + field.size + ')':'(10)' );
            break;
        case 'URL':
        case 'Text':
        case 'Note':
            s =field.size ? util.format('NVARCHAR2(%s)', field.size) : 'NVARCHAR2(255)';
            break;
        case 'Image':
        case 'Binary':
            s ='BLOB';
            break;
        case 'Guid':
            s = 'VARCHAR2(36)';
            break;
        case 'Short':
            s = 'NUMBER(2,0)';
            break;
        default:
            s = 'NUMBER';
            break;
    }
    if (field.primary) {
        return s.concat(' NOT NULL');
    }
    else {
        return s.concat((typeof field.nullable=== 'undefined' || field.nullable == null) ? ' NULL': (field.nullable ? ' NULL': ' NOT NULL'));
    }
};

/**
 * Begins a transactional operation by executing the given function
 * @param fn {function} The function to execute
 * @param callback {function(Error=)} The callback that contains the error -if any- and the results of the given operation
 */
OracleAdapter.prototype.executeInTransaction = function(fn, callback) {
    var self = this;
    //ensure parameters
    fn = fn || function() {}; callback = callback || function() {};
    self.open(function(err) {
        if (err) {
            callback(err);
        }
        else {
            if (self.transaction) {
                fn.call(self, function(err) {
                    callback(err);
                });
            }
            else {
                //initialize dummy transaction object (for future use)
                self.transaction = { };
                //execute function
                fn.call(self, function(err) {
                    if (err) {
                        //rollback transaction
                        self.rawConnection.rollback(function() {
                            delete self.transaction;
                            callback(err);
                        });
                    }
                    else {
                        //commit transaction
                        self.rawConnection.commit(function(err) {
                            delete self.transaction;
                            callback(err);
                        });
                    }
                });
            }
        }
    });
};

/**
 *
 * @param {string} name
 * @param {QueryExpression|*} query
 * @param {function(Error=)} callback
 */
OracleAdapter.prototype.createView = function(name, query, callback) {
    this.view(name).create(query, callback);
};


/*
 * @param {DataModelMigration|*} obj An Object that represents the data model scheme we want to migrate
 * @param {function(Error=)} callback
 */
OracleAdapter.prototype.migrate = function(obj, callback) {
    var self = this;
    callback = callback || function() {};
    if (typeof obj === 'undefined' || obj == null) { callback(); return; }
    /**
     * @type {DataModelMigration|*}
     */
    var migration = obj;

    var format = function(format, obj)
    {
        var result = format;
        if (/%t/.test(format))
            result = result.replace(/%t/g,OracleAdapter.formatType(obj));
        if (/%f/.test(format))
            result = result.replace(/%f/g,obj.name);
        return result;
    };


    async.waterfall([
        //1. Check migrations table existence
        function(cb) {
            if (OracleAdapter.supportMigrations) {
                cb(null, true);
                return;
            }
            self.table('migrations').exists(function(err, exists) {
                if (err) { cb(err); return; }
                cb(null, exists);
            });
        },
        //2. Create migrations table, if it does not exist
        function(arg, cb) {
            if (arg) { cb(null, 0); return; }
            //create migrations table

            async.eachSeries([
                'CREATE TABLE "migrations"("appliesTo" NVARCHAR2(255) NOT NULL, "model" NVARCHAR2(255) NULL, ' +
                '"description" NVARCHAR2(255),"version" NVARCHAR2(24) NOT NULL)'
            ], function(s, cb0) {
                self.execute(s, [], cb0)
            }, function(err) {
                if (err) { return cb(err); }
                OracleAdapter.supportMigrations=true;
                return cb(null, 0);
            });

            //self.execute('CREATE TABLE "migrations"("id" NUMBER(10) NOT NULL, ' +
            //    '"appliesTo" NVARCHAR2(255) NOT NULL, "model" NVARCHAR2(255) NULL, ' +
            //    '"description" NVARCHAR2(255),"version" NVARCHAR2(24) NOT NULL, ' +
            //    'CONSTRAINT "migrations_pk" PRIMARY KEY ("id")); ' +
            //    'CREATE SEQUENCE "migrations_seq" START WITH 1 INCREMENT BY 1; ' +
            //    'CREATE TRIGGER "migrations_auto_inc" BEFORE INSERT ON "migrations" FOR EACH ROW BEGIN :new."id" := "migrations_seq".nextval; END;',
            //    [], function(err) {
            //        if (err) { cb(err); return; }
            //        OracleAdapter.supportMigrations=true;
            //        cb(null, 0);
            //    });
        },
        //3. Check if migration has already been applied (true=Table version is equal to migration version, false=Table version is older from migration version)
        function(arg, cb) {
            self.table(migration.appliesTo).version(function(err, version) {
                if (err) { cb(err); return; }
                cb(null, (version>=migration.version));
            });
        },
        //4a. Check table existence (-1=Migration has already been applied, 0=Table does not exist, 1=Table exists)
        function(arg, cb) {
            //migration has already been applied (set migration.updated=true)
            if (arg) {
                migration['updated']=true;
                cb(null, -1);
            }
            else {
                self.table(migration.appliesTo).exists(function(err, exists) {
                    if (err) { cb(err); return; }
                    cb(null, exists ? 1 : 0);
                });

            }
        },
        //4. Get table columns
        function(arg, cb) {
            //migration has already been applied
            if (arg<0) { cb(null, [arg, null]); return; }
            self.table(migration.appliesTo).columns(function(err, columns) {
                if (err) { cb(err); return; }
                cb(null, [arg, columns]);
            });
        },
        //5. Migrate target table (create or alter)
        function(args, cb) {
            //migration has already been applied (args[0]=-1)
            if (args[0] < 0) {
                cb(null, args[0]);
            }
            else if (args[0] == 0) {
                //create table
                var strFields = migration.add.filter(function(x) {
                    return !x['oneToMany']
                }).map(
                    function(x) {
                        return format('"%f" %t', x);
                    }).join(', ');
                var sql = util.format('CREATE TABLE "%s" (%s)', migration.appliesTo, strFields);
                self.execute(sql, null, function(err) {
                    if (err) { cb(err); return; }
                    cb(null, 1);
                });
            }
            else if (args[0] == 1) {

                var expressions = [],
                    /**
                     * @type {{columnName:string,ordinal:number,dataType:*, maxLength:number,isNullable:number,,primary:boolean }[]}
                     */
                    columns = args[1], forceAlter = false, column, newType, oldType;
                //validate operations

                //1. columns to be removed
                if (util.isArray(migration.remove)) {
                    if (migration.remove>0) {
                        for (var i = 0; i < migration.remove.length; i++) {
                            var x = migration.remove[i];
                            var colIndex = columns.indexOf(function(y) { return y.name== x.name; });
                            if (colIndex>=0) {
                                if (!columns[colIndex].primary) {
                                    forceAlter = true;
                                }
                                else {
                                    migration.remove.splice(i, 1);
                                    i-=1;
                                }
                            }
                            else {
                                migration.remove.splice(i, 1);
                                i-=1;
                            }
                        }
                    }
                }
                //1. columns to be changed
                if (util.isArray(migration.change)) {
                    if (migration.change>0) {

                        for (var i = 0; i < migration.change.length; i++) {
                            var x = migration.change[i];
                            column = columns.find(function(y) { return y.name==x.name; });
                            if (column) {
                                if (!column.primary) {
                                    //validate new column type (e.g. TEXT(120,0) NOT NULL)
                                    newType = format('%t', x); oldType = column.type.toUpperCase().concat(column.nullable ? ' NOT NULL' : ' NULL');
                                    if ((newType!=oldType)) {
                                        //force alter
                                        forceAlter = true;
                                    }
                                }
                                else {
                                    //remove column from change collection (because it's a primary key)
                                    migration.change.splice(i, 1);
                                    i-=1;
                                }
                            }
                            else {
                                //add column (column was not found in table)
                                migration.add.push(x);
                                //remove column from change collection
                                migration.change.splice(i, 1);
                                i-=1;
                            }

                        }

                    }
                }
                if (util.isArray(migration.add)) {

                    for (var i = 0; i < migration.add.length; i++) {
                        var x = migration.add[i];
                        column = columns.find(function(y) { return (y.name==x.name); });
                        if (column) {
                            if (column.primary) {
                                migration.add.splice(i, 1);
                                i-=1;
                            }
                            else {
                                newType = format('%t', x); oldType = column.type.toUpperCase().concat(column.nullable ? ' NOT NULL' : ' NULL');
                                if (newType==oldType) {
                                    //remove column from add collection
                                    migration.add.splice(i, 1);
                                    i-=1;
                                }
                                else {
                                    forceAlter = true;
                                }
                            }
                        }
                    }
                    if (forceAlter) {
                        cb(new Error('Full table migration is not yet implemented.'));
                        return;
                    }
                    else {
                        migration.add.forEach(function(x) {
                            //search for columns
                            expressions.push(util.format('ALTER TABLE "%s" ADD COLUMN "%s" %s', migration.appliesTo, x.name, OracleAdapter.formatType(x)));
                        });
                    }

                }
                if (expressions.length>0) {
                    self.execute(expressions.join(';'), [], function(err) {
                        if (err) { cb(err); return; }
                        cb(null, 1);
                    });
                }
                else {
                    cb(null, 2);
                }
            }
            else {
                cb(new Error('Invalid table status.'));
            }
        },
        function(arg, cb) {
            if (arg>0) {
                //log migration to database
                self.execute('INSERT INTO "migrations"("appliesTo", "model", "version", "description") VALUES (?,?,?,?)', [migration.appliesTo,
                    migration.model,
                    migration.version,
                    migration.description ], function(err, result) {
                    if (err)  {
                        cb(err);
                        return;
                    }
                    cb(null, 1);
                });
            }
            else {
                migration['updated'] = true;
                cb(null, arg);
            }
        }
    ], function(err) {
        callback(err);
    })

};

/**
 * Produces a new identity value for the given entity and attribute.
 * @param entity {String} The target entity name
 * @param attribute {String} The target attribute
 * @param callback {Function=}
 */
OracleAdapter.prototype.selectIdentity = function(entity, attribute , callback) {

    var self = this;

    var migration = {
        appliesTo:'increment_id',
        model:'increments',
        description:'Increments migration (version 1.0)',
        version:'1.0',
        add:[
            { name:'id', type:'Counter', primary:true },
            { name:'entity', type:'Text', size:120 },
            { name:'attribute', type:'Text', size:120 },
            { name:'value', type:'Integer' }
        ]
    }
    //ensure increments entity
    self.migrate(migration, function(err)
    {
        //throw error if any
        if (err) { callback.call(self,err); return; }
        self.execute('SELECT * FROM increment_id WHERE entity=? AND attribute=?', [entity, attribute], function(err, result) {
            if (err) { callback.call(self,err); return; }
            if (result.length==0) {
                //get max value by querying the given entity
                var q = qry.query(entity).select([qry.fields.max(attribute)]);
                self.execute(q,null, function(err, result) {
                    if (err) { callback.call(self, err); return; }
                    var value = 1;
                    if (result.length>0) {
                        value = parseInt(result[0][attribute]) + 1;
                    }
                    self.execute('INSERT INTO increment_id(entity, attribute, value) VALUES (?,?,?)',[entity, attribute, value], function(err) {
                        //throw error if any
                        if (err) { callback.call(self, err); return; }
                        //return new increment value
                        callback.call(self, err, value);
                    });
                });
            }
            else {
                //get new increment value
                var value = parseInt(result[0].value) + 1;
                self.execute('UPDATE increment_id SET value=? WHERE id=?',[value, result[0].id], function(err) {
                    //throw error if any
                    if (err) { callback.call(self, err); return; }
                    //return new increment value
                    callback.call(self, err, value);
                });
            }
        });
    });
};

/**
 * Executes an operation against database and returns the results.
 * @param {DataModelBatch} batch
 * @param {function(Error=)} callback
 */
OracleAdapter.prototype.executeBatch = function(batch, callback) {
    callback = callback || function() {};
    callback(new Error('DataAdapter.executeBatch() is obsolete. Use DataAdapter.executeInTransaction() instead.'));
};

OracleAdapter.prototype.table = function(name) {
    var self = this, owner, table;
    var matches = /(\w+)\.(\w+)/.exec(name);
    if (matches) {
        //get schema owner
        owner = matches[1];
        //get table name
        table = matches[2];
    }
    else {
        table = name;
    }
    return {
        /**
         * @param {function(Error,Boolean=)} callback
         */
        exists:function(callback) {
            var sql;
            if (typeof owner === 'undefined' || owner == null) {
                sql = 'SELECT COUNT(*) AS "count" FROM ALL_OBJECTS WHERE object_type IN (\'TABLE\') AND object_name = ?';
            }
            else {
                sql = 'SELECT COUNT(*) AS "count" FROM ALL_OBJECTS WHERE object_type IN (\'TABLE\') AND object_name = ? AND REGEXP_LIKE(owner,?,\'i\')';
            }
            self.execute(sql, [table, '^' + owner + '$'], function(err, result) {
                if (err) { callback(err); return; }
                callback(null, (result[0].count>0));
            });
        },
        /**
         * @param {function(Error,string=)} callback
         */
        version:function(callback) {
            self.execute('SELECT MAX("version") AS "version" FROM "migrations" WHERE "appliesTo"=?',
                [name], function(err, result) {
                    if (err) { cb(err); return; }
                    if (result.length==0)
                        callback(null, '0.0');
                    else
                        callback(null, result[0].version || '0.0');
                });
        },
        /**
         * @param {function(Error,Boolean=)} callback
         */
        hasSequence:function(callback) {
            callback = callback || function() {};
            self.execute('SELECT COUNT(*) AS "count" FROM ALL_SEQUENCES WHERE SEQUENCE_NAME=?',
                [ table + '_seq' ], function(err, result) {
                    if (err) { callback(err); return; }
                    callback(null, (result[0].count>0));
                });
        },
        /**
         * @param {function(Error=,Array=)} callback
         */
        columns:function(callback) {
            callback = callback || function() {};

            /*
             SELECT c0.COLUMN_NAME AS "name", c0.DATA_TYPE AS "type", ROWNUM AS "ordinal",
             c0.DATA_LENGTH AS "size", c0.DATA_SCALE AS "scale", CASE WHEN c0.NULLABLE='Y'
             THEN 1 ELSE 0 END AS "nullable", CASE WHEN t0.CONSTRAINT_TYPE='P' THEN 1 ELSE 0 END AS "primaryKey"
             FROM ALL_TAB_COLUMNS c0 LEFT JOIN (SELECT cols.table_name, cols.column_name, cols.owner, cons.constraint_type
             FROM all_constraints cons, all_cons_columns cols WHERE cons.constraint_type = 'P'
             AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner) t0 ON c0.TABLE_NAME=t0.TABLE_NAME
             AND c0.OWNER=t0.OWNER AND c0.COLUMN_NAME=t0.COLUMN_NAME WHERE c0.TABLE_NAME = ?
            */

            var sql = 'SELECT c0.COLUMN_NAME AS "name", c0.DATA_TYPE AS "type", ROWNUM AS "ordinal", c0.DATA_LENGTH AS "size", ' +
                'c0.DATA_SCALE AS "scale", CASE WHEN c0.NULLABLE=\'Y\' THEN 1 ELSE 0 END AS "nullable", CASE WHEN t0.CONSTRAINT_TYPE=\'P\' ' +
            'THEN 1 ELSE 0 END AS "primary" FROM ALL_TAB_COLUMNS c0 LEFT JOIN (SELECT cols.table_name, cols.column_name, cols.owner, ' +
            'cons.constraint_type FROM all_constraints cons, all_cons_columns cols WHERE cons.constraint_type = \'P\' ' +
            'AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner) t0 ON c0.TABLE_NAME=t0.TABLE_NAME ' +
            'AND c0.OWNER=t0.OWNER AND c0.COLUMN_NAME=t0.COLUMN_NAME WHERE c0.TABLE_NAME = ?';
            if (owner) { sql += ' AND REGEXP_LIKE(c0.OWNER,?,\'i\')'; }
            self.execute(sql, [name, '^' + owner + '$'], function(err, result) {
                    if (err) { callback(err); return; }
                    callback(null, result);
                });
        }
    }

};

OracleAdapter.prototype.view = function(name) {
    var self = this, owner, view;
    var matches = /(\w+)\.(\w+)/.exec(name);
    if (matches) {
        //get schema owner
        owner = matches[1];
        //get table name
        view = matches[2];
    }
    else {
        view = name;
    }
    return {
        /**
         * @param {function(Error,Boolean=)} callback
         */
        exists:function(callback) {
            var sql = 'SELECT COUNT(*) AS "count" FROM ALL_OBJECTS WHERE object_type IN (\'VIEW\') AND object_name = ?';
            if (typeof owner !== 'undefined') {
                sql += ' AND REGEXP_LIKE(owner,?,\'i\')';
            }
            self.execute(sql, [name, '^' + (owner || '') + '$'], function(err, result) {
                if (err) { callback(err); return; }
                callback(null, (result[0].count>0));
            });
        },
        /**
         * @param {function(Error=)} callback
         */
        drop:function(callback) {
            callback = callback || function() {};
            self.open(function(err) {
               if (err) { return callback(err); }

                var sql = 'SELECT COUNT(*) AS "count" FROM ALL_OBJECTS WHERE object_type IN (\'VIEW\') AND object_name = ?';
                if (typeof owner !== 'undefined') {
                    sql += ' AND REGEXP_LIKE(owner,?,\'i\')';
                }
                self.execute(sql, [name, '^' + (owner || '') + '$'], function(err, result) {
                    if (err) { return callback(err); }
                    var exists = (result[0].count>0);
                    if (exists) {
                        var sql = util.format('DROP VIEW "%s"',name);
                        self.execute(sql, undefined, function(err) {
                            if (err) { callback(err); return; }
                            callback();
                        });
                    }
                    else {
                        callback();
                    }
                });
            });
        },
        /**
         * @param {QueryExpression|*} q
         * @param {function(Error=)} callback
         */
        create:function(q, callback) {
            var thisArg = this;
            self.executeInTransaction(function(tr) {
                thisArg.drop(function(err) {
                    if (err) { tr(err); return; }
                    try {
                        var sql = util.format('CREATE VIEW "%s" AS ',name);
                        var formatter = new OracleFormatter();
                        sql += formatter.format(q);
                        self.execute(sql, undefined, tr);
                    }
                    catch(e) {
                        tr(e);
                    }
                });
            }, function(err) {
                callback(err);
            });

        }
    };
};

/**
 * Executes a query against the underlying database
 * @param query {QueryExpression|string|*}
 * @param values {*=}
 * @param {function(Error=,*=)} callback
 */
OracleAdapter.prototype.execute = function(query, values, callback) {
    var self = this, sql = null;
    try {

        if (typeof query == 'string') {
            //get raw sql statement
            sql = query;
        }
        else {
            //format query expression or any object that may be act as query expression
            var formatter = new OracleFormatter();
            sql = formatter.format(query);
        }
        //validate sql statement
        if (typeof sql !== 'string') {
            callback.call(self, new Error('The executing command is of the wrong type or empty.'));
            return;
        }
        //ensure connection
        self.open(function(err) {
            if (err) {
                callback.call(self, err);
            }
            else {
                //log statement (optional)
                if (process.env.NODE_ENV==='development')
                    console.log(util.format('SQL:%s, Parameters:%s', sql, JSON.stringify(values)));
                //prepare statement - the traditional way
                var prepared = self.prepare(sql, values);
                //execute raw command
                self.rawConnection.execute(prepared,[], {outFormat: oracledb.OBJECT, autoCommit: (typeof self.transaction === 'undefined') }, function(err, result) {
                    if (err) {
                        //log sql
                        console.log(util.format('SQL Error:%s', prepared));
                        callback(err);
                    }
                    else {
                        if (result)
                            callback(null, result.rows);
                        else
                            callback();
                    }
                });
            }
        });
    }
    catch (e) {
        callback.call(self, e);
    }

};

/**
 * @class OracleFormatter
 * @constructor
 * @augments {SqlFormatter}
 */
function OracleFormatter() {
    this.settings = {
        nameFormat:OracleFormatter.NAME_FORMAT,
        forceAlias:true
    }
}
util.inherits(OracleFormatter, qry.classes.SqlFormatter);

OracleFormatter.NAME_FORMAT = '"$1"';

OracleFormatter.prototype.escapeName = function(name) {
    if (typeof name === 'string')
        return name.replace(/(\w+)/ig, this.settings.nameFormat);
    return name;
};

var REGEXP_SINGLE_QUOTE=/\\'/g, SINGLE_QUOTE_ESCAPE ='\'\'',
    REGEXP_DOUBLE_QUOTE=/\\"/g, DOUBLE_QUOTE_ESCAPE = '"',
    REGEXP_SLASH=/\\\\/g, SLASH_ESCAPE = '\\';
/**
 * Escapes an object or a value and returns the equivalent sql value.
 * @param {*} value - A value that is going to be escaped for SQL statements
 * @param {boolean=} unquoted - An optional value that indicates whether the resulted string will be quoted or not.
 * returns {string} - The equivalent SQL string value
 */
OracleFormatter.prototype.escape = function(value,unquoted)
{
    if (typeof value === 'boolean') { return value ? '1' : '0'; }
    var res = OracleFormatter.super_.prototype.escape.call(this, value, unquoted);
    if (typeof value === 'string') {
        if (/\\'/g.test(res))
        //escape single quote (that is already escaped)
            res = res.replace(/\\'/g, SINGLE_QUOTE_ESCAPE);
        if (/\\"/g.test(res))
        //escape double quote (that is already escaped)
            res = res.replace(/\\"/g, DOUBLE_QUOTE_ESCAPE);
        if (/\\\\/g.test(res))
        //escape slash (that is already escaped)
            res = res.replace(/\\\\/g, SLASH_ESCAPE);
    }
    return res;
};

/**
 * Implements indexOf(str,substr) expression formatter.
 * @param {string} p0 The source string
 * @param {string} p1 The string to search for
 * @returns {string}
 */
OracleFormatter.prototype.$indexof = function(p0, p1)
{
    return util.format('(INSTR(%s,%s)-1)', this.escape(p0), this.escape(p1));
};

/**
 * Implements concat(a,b) expression formatter.
 * @param {*} p0
 * @param {*} p1
 * @returns {string}
 */
OracleFormatter.prototype.$concat = function(p0, p1)
{
    return util.format('CONCAT(%s,%s)', this.escape(p0),  this.escape(p1));
};

/**
 * Implements substring(str,pos) expression formatter.
 * @param {String} p0 The source string
 * @param {Number} pos The starting position
 * @param {Number=} length The length of the resulted string
 * @returns {string}
 */
OracleFormatter.prototype.$substring = function(p0, pos, length)
{
    if (length)
        return util.format('SUBSTR(%s,%s,%s)', this.escape(p0), pos.valueOf()+1, length.valueOf());
    else
        return util.format('SUBSTR(%s,%s)', this.escape(p0), pos.valueOf()+1);
};

/**
 * Implements length(a) expression formatter.
 * @param {*} p0
 * @returns {string}
 */
OracleFormatter.prototype.$length = function(p0) {
    return util.format('LENGTH(%s)', this.escape(p0));
};

OracleFormatter.prototype.$ceiling = function(p0) {
    return util.format('CEIL(%s)', this.escape(p0));
};

OracleFormatter.prototype.$startswith = function(p0, p1)
{
    //validate params
    if (Object.isNullOrUndefined(p0) || Object.isNullOrUndefined(p1))
        return '';
    return 'REGEXP_LIKE(owner,' + this.escape(p0) + ',\'^' + this.escape(p1, true) + '\')';
};

OracleFormatter.prototype.$contains = function(p0, p1)
{
    //validate params
    if (Object.isNullOrUndefined(p0) || Object.isNullOrUndefined(p1))
        return '';
    return 'REGEXP_LIKE(owner,' + this.escape(p0) + ',\'' + this.escape(p1, true) + '\')';
};

OracleFormatter.prototype.$endswith = function(p0, p1)
{
    //validate params
    if (Object.isNullOrUndefined(p0) || Object.isNullOrUndefined(p1))
        return '';
    return 'REGEXP_LIKE(owner,' + this.escape(p0) + ',\'' + this.escape(p1, true) + '$\')';
};

OracleFormatter.prototype.$day = function(p0) { return util.format('CAST(TO_CHAR(%s,\'DD\') AS NUMBER)', this.escape(p0)) ; };
OracleFormatter.prototype.$month = function(p0) { return util.format('CAST(TO_CHAR(%s,\'MM\') AS NUMBER)', this.escape(p0)) ; };
OracleFormatter.prototype.$year = function(p0) { return util.format('CAST(TO_CHAR(%s,\'YYYY\') AS NUMBER)', this.escape(p0)) ; };
OracleFormatter.prototype.$hour = function(p0) { return util.format('CAST(TO_CHAR(%s,\'HH24\') AS NUMBER)', this.escape(p0)) ; };
OracleFormatter.prototype.$minute = function(p0) { return util.format('CAST(TO_CHAR(%s,\'MI\') AS NUMBER)', this.escape(p0)) ; };
OracleFormatter.prototype.$second = function(p0) { return util.format('CAST(TO_CHAR(%s,\'SS\') AS NUMBER)', this.escape(p0)) ; };
OracleFormatter.prototype.$date = function(p0) { return util.format('TO_TIMESTAMP_TZ(TO_CHAR(%s, \'YYYY-MM-DD\'),\'YYYY-MM-DD\')', this.escape(p0)) ; };

var orsql = {
    /**
     * @constructs OracleAdapter
     * */
    OracleAdapter : OracleAdapter,
    /**
     * Creates an instance of OracleAdapter object that represents an Oracle database connection.
     * @param {*} options An object that represents the properties of the underlying database connection.
     * @returns {DataAdapter|*}
     */
    createInstance: function(options) {
        return new OracleAdapter(options);
    }
};

if (typeof exports !== 'undefined')
{
    module.exports = orsql;
}

