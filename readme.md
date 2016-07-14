postgresjs
=================
PostgreSQL database wrapper that provides helpers to query the database.

* Select, Insert, Update and Delete Query Builders
* Merge Command allowing insert or update
* Suspend integration for generator-based async control-flow
* Idle Connection Auto Closer 
* No transpiling required

### requirements

* ECMAScript 2015 (ES6)
* Node.JS 6.2.2 or later (tested on 6.2.2)

### jsdoc documentation

[https://cdn.rawgit.com/phanxgames/postgresjs/master/jsdoc/](https://cdn.rawgit.com/phanxgames/postgresjs/master/jsdoc/Postgresjs.html)
 

### install

```
npm install postgresjs
```

Copy the dbConfig.ex.json file into your project source folder, rename to dbConfig.json,
and update with your database connection information.


### asynchronous nature

All methods that include a callback (cb) have been designed to be used with the
 suspend library and may be placed behind a yield command. Be sure to leave the cb
 parameter null to use the suspend.resume functionality automatically.
 
IMPORTANT: You will also need to set the resume reference in the constructor or the
 setResume() method, before the suspend.resume functionality will
  be enabled.
 
If you do provide a callback, the 3rd parameter, "next" (ex: cb(err,result, next))
 will be the suspend.resume function reference so you may resume execution
 to move past the next yield command.

 
### basic example

```
	var suspend = require("suspend");
	var Postgresjs = require("postgresjs");

	//Attach your dbConfig to the Postgresjs module
	Postgresjs.config = require('./dbConfig.json');

	var db = new Postgresjs(suspend.resume);

	suspend(function*() {
	   yield db.start();
	   
	   //... place query methods here ...
	   
	   yield db.end();
	});
```

 
### reading rows

There are 3 ways to loop over the resulting rows of a query.

1) Standard callback.

```
	yield db.query("select email from users where id=? ;",[userid],
		function(err,rows) {
			for (let row of rows) {
				//..
			}
	});
```

2) Rows Getter.

```
	yield db.query("select email from users where id=? ;",[userid]);	
	
	for (let row of rows) {
		//..
	}
```

3) Async Looping (non-blocking)

```
	yield db.query("select email from users where id=? ;",[userid]);
	
	yield db.asyncForEach(function(index,row,cbNext) {
		//..
	});

```

### checking for errors and rowcount

After every query you should check if it was an error.
```
	if (db.error()) {
		console.error("Database error: ",db.error());
		return;
	}
```

And you may also want to check how many rows were returned before looping.
```
	if (db.rowCount > 0) {
		//.. loop
		
	} else {
		console.log("No rows found.");
	}
```
		

### auto closer

Enabling auto closer in the dbConfig.json file allows database connections that you
leave open to be automatically close after a timeout interval provided in minutes.

By default this is not enabled, however you may want to keep this enabled and watch the
console to see if the auto closer picks up on any open connections so you can address
it and proprely close it when you are done.

 
### query helpers examples

The Helpers will help you build SQL statements and provide parameterized values which
safeguard your queries from SQL injections. Any property that is labled as "value" will
be converted to a parameter internally.

##### Select Helper
```
	//..
	yield db.selectHelper({
		 table:"users",
		 columns:["username","email"],
		 where: db.whereHelper({
			 "username -like":"h%"
		 }),
		 orderBy: db.orderByHelper({
			 "email":"ASC"
		 })
	});
		 
	for (let row of db.rows) {
		 console.log(row);
		 //Output example: {username:"Tester",email:"test@test.com"}
	}
	//..
```

##### Insert Helper
```
	//..
	yield db.insertHelper({
	   table:"users",
	   columns:{
		   "username":"tester",
		   "email":"oldemail@test.com"
	   }
	});
	//..
```
 
##### Update Helper
```
	//..
	yield db.updateHelper({
	  table: "users",
	  columns: {
		  "email":"newemail@test.com"
	  },
	  where: db.whereHelper({
		  "username":"tester"
	  })
	});
	//..
```

##### Delete Helper
```
	//..
	yield db.deleteHelper({
	  table: "users",
	  where: db.whereHelper({
		  "username":"tester"
	  })
	});
	//..
```

##### Merge Helper
```
	//..
	yield db.mergeHelper({
	  table: "users",
	  columns: {
		  "username":"tester",
		  "email":"test@test.com"
	  },
	  where: db.whereHelper({
		  "username":"tester"
	  })
	});
	//..
```


### Module Dependencies

- [node-postgres](https://github.com/brianc/node-postgres)
- [suspend](https://github.com/jmar777/suspend)
- [dictionaryjs](https://github.com/phanxgames/dictionaryjs)


