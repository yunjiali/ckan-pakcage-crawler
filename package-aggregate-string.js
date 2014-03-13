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


if(argv.help){
	console.info("--field		The JSON field in the packages collection that you want to analysis, such as 'metadata.license_id'");
	return;
	process.exit(0);
}

if(!argv.field){
	console.info("Must use --field");
	return;
	process.exit(0);
}

var mongourl = "mongodb://";
if(CONFIG.dbUsername && CONFIG.dbPassword)
	mongourl=CONFIG.dbUsername+":"+CONFIG.dbPassword+"@";
mongourl+=CONFIG.dbHost+":"+CONFIG.dbPort+"/"+CONFIG.dbName;

//this is tricky, but saves us a lot of time
eval("var mapFunc = function(){emit({field:this."+argv.field+"},{count:1});}");

var reduceFunc = function(key,values){
	var count = 0;
	values.forEach(function(v){
		count+=v['count'];
	});

	return {count:count};
}

//var db = monk(mongourl);
MongoClient.connect(mongourl, function(err,db){
	console.info("Connected to MongoDB.");
	db.collection("packages").mapReduce(mapFunc,reduceFunc,{
		out:{inline:1},
		scope:{field:argv.field}
	}, function(error, results, stats){
			if(error){
				console.error("Error:"+error);
				return;
				process.exit(0);
			}
			else{
				console.log(results);
				fs.appendFile("results/"+argv.field+"-group.csv","value,count\r\n");
				for(var i=0;i<results.length;i++){

					var csvLine = "";
					if(results[i]._id.field === undefined){
						csvLine = "undefined,"+results[i].value.count+"\r\n";
					}
					else if(results[i]._id.field === null){
						csvLine = "null,"+results[i].value.count+"\r\n";
					}
					else{
						csvLine = results[i]._id.field.replace(/,/g,';').replace(/\r\n/,'.')+","+results[i].value.count+"\r\n";
					}
					fs.appendFile("results/"+argv.field+"-group.csv",csvLine);
				}

				console.log("finish");
			}

			

	});

	
});






