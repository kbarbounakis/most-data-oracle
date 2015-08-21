# most-data-oracle
Most Web Framework Oracle Adapter
##Install
$ npm install most-data-oracle
##Usage
Register Oracle adapter on app.json as follows:

    "adapterTypes": [
        ...
        { "name":"Oracle Data Adapter", "invariantName": "oracle", "type":"most-data-oracle" }
        ...
    ],
    adapters: [
        ...
        { "name":"development", "invariantName":"oracle", "default":true,
            "options": {
              "host":"localhost",
              "port":1521,
              "user":"user",
              "password":"password",
              "service":"orcl",
              "schema":"public"
            }
        }
        ...
    ]

If you are intended to use Oracle adapter as the default database adapter set the property "default" to true. 
