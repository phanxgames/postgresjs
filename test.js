var suspend = require("suspend"),
    resume = suspend.resume;


var Postgresjs = require("./Postgresjs");
Postgresjs.config = require('./tests/dbConfig.json');


var db = new Postgresjs(resume);

suspend(function*() {

    db.throwErrors = false;

    yield db.start();

    let result = yield db.selectRow("select id,named from characters where id=? ;",[2]);

    if (result!=null) {

        console.log(result);

        console.log(result.name);
    }

    return;

    yield db.query("select name from characters limit 20 ;");

    if (db.error()) {
        console.log("error:",db.error);
        db.end();
        return;
    }

    console.log(db.rowCount);

    console.log(db.rows);




    yield db.query("select name, class_id from characters order by class_id ASC ;",null,(err,result,next) => {
        console.log("result",result);
        next();
    });

    yield db.query("select name from characters ;");

    if (db.error()) {
        console.log("error:",db.error);
        db.end();
        return;
    }

    var rows = db.rows; //array
    console.log("rows",rows);

    var i = 0;
    for (var value of db.rows) {
        console.log(i,value);
        i++;
    }

    console.log("");
    console.log("Start of asyncForEach:");
    yield db.asyncForEach((index,item,next) => {
        console.log(item);
        next();
    });
    console.log("End of asyncForEach");
    console.log("");




    yield db.end();

    console.log("end of db test");


})();

