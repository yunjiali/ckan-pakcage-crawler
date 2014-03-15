var http=require('http');
var _=require('underscore');
var mongo = require('mongodb');
var MongoClient = mongo.MongoClient;
var ObjectID = mongo.ObjectID;
var monk = require('monk');
var async = require('async');
var S=require('string');
var CONFIG=require('config').Crawler;
var	request=require('request');
var jf = require('jsonfile');
var argv=require('optimist').argv
var winston = require('winston');
var fs = require('fs');


/*if(argv.help){
	console.info("--field		The JSON field in the packages collection that you want to analysis, such as 'metadata.license_id'");
	return;
	process.exit(0);
}

if(!argv.field){
	console.info("Must use --field");
	return;
	process.exit(0);
}*/

var mongourl = "mongodb://";
if(CONFIG.dbUsername && CONFIG.dbPassword)
	mongourl=CONFIG.dbUsername+":"+CONFIG.dbPassword+"@";
mongourl+=CONFIG.dbHost+":"+CONFIG.dbPort+"/"+CONFIG.dbName;

//this is tricky, but saves us a lot of time
//eval("var mapFunc = function(){if(!this."+argv.field+"){return;}"+
//	"for(index in this."+argv.field+"){emit(this."+argv.field+"[index],1);}}");
var mapFunc = function(){
	if(!this.metadata.tags){
		return;
	}
	for(index in this.metadata.tags)
	{
		emit(this.metadata.tags[index].name,1);
	}
};

var reduceFunc = function(key,values){
	var count = 0;
	for(index in values)
	{
		count+=values[index];
	}

	return count;
}

MongoClient.connect(mongourl, function(err,db){
	console.info("Connected to MongoDB.");
	db.collection("packages").mapReduce(mapFunc,reduceFunc,{
		out:"ckantags",
		scope:{field:argv.field}
	}, function(error, results, stats){
			if(error){
				console.error("Error:"+error);
				return;
				process.exit(0);
			}
			else{
				//console.log(results);
				fs.appendFileSync("results/metadata.tags-group.csv","value,count\r\n");
				db.collection("ckantags").find().toArray(function(error, results){
					for(var i=0;i<results.length;i++){

						var csvLine = "";
						if(results[i]._id === undefined){
							csvLine = "undefined,"+results[i].value.count+"\r\n";
						}
						else if(results[i]._id === null){
							csvLine = "null,"+results[i].value+"\r\n";
						}
						else{
							csvLine = results[i]._id.replace(/,/g,';').replace(/\r\n/,'.')+","+results[i].value+"\r\n";
						}
						fs.appendFileSync("results/metadata.tags-group.csv",csvLine);
					}

					console.log("finish");
				});
				
			}

	});
	
});