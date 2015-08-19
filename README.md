# most-data-oracle (in progress)
Most Web Framework Oracle Adapter
##Install
$ npm install most-data-oracle
##Usage
Register Oracle adapter on app.json as follows:

    adapters: {
        "postgres": { "name":"development", "invariantName":"oracle", "default":true,
            "options": {
              "host":"localhost",
              "post":1521,
              "user":"user",
              "password":"password",
              "sid":"orcl",
              "schema":"public"
            }
    }
}

If you are intended to use Oracle adapter as the default database adapter set the property "default" to true. 
