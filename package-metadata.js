//get package metadata
//get url: http://base_api_url/action/package_show?id=xxxx

var http=require('http');
var _=require('underscore');
var mongo = require('mongodb');
var ObjectID = mongo.ObjectID;
var monk = require('monk');
var async = require('async');
var S=require('string');
var CONFIG=require('config').Crawler;
var	request=require('request');
var jf = require('jsonfile');
var argv=require('optimist').argv

if(argv.help){
	console.info("--id		The ID of the website that you want to get package metadata, such as 'data_gov_uk'.");
	console.info("--all		Get the package metadata for all the websites in the database.");
	return;
	process.exit(0);
}

var mongourl = "";
if(CONFIG.dbUsername && CONFIG.dbPassword)
	mongourl=CONFIG.dbUsername+":"+CONFIG.dbPassword+"@";
mongourl+=CONFIG.dbHost+":"+CONFIG.dbPort+"/"+CONFIG.dbName;
var db = monk(mongourl);
console.info("Connected to MongoDB.");
console.info("Create Index.");
//db.get('instances').ensureIndex({website_id:1},{unique:true});
db.get('instances').ensureIndex({website_id:1, package_id:1}); 


var packageArray=[];

if(argv.id){
	db.get("instances").findOne({id:argv.id, package_list:{$exists:true}},
		{id:true,api_base_url:true,package_list:true},function(errInstance, instance){
			
			for(var j=0;j<instance.package_list.length;j++){
				var packageObj={};
				packageObj.website_id = instance.id;
				packageObj.package_id = instance.package_list[j];
				packageObj.metadata_url = instance.api_base_url+"/action/package_show?id="+instance.package_list[j];
				packageArray.push(packageObj);
			}
			
			console.log(packageArray.length+" packages.");

		}
	)
}
else if(argv.all){
	db.get("instances").find({package_list:{$exists:true}},
		{id:true,api_base_url:true,package_list:true},function(errInstances, instances){
			console.log(instances.length+" instances found.");
			for(var i=0;i<instances.length;i++){
				var instance = instances[i];
				for(var j=0;j<instance.package_list.length;j++){
					var packageObj={};
					packageObj.website_id = instance.id;
					packageObj.package_id = instance.package_list[j];
					packageObj.metadata_url = instance.api_base_url+"/action/package_show?id="+instance.package_list[j];
					packageArray.push(packageObj);
				}
			}
			console.log(packageArray.length+" packages.");

		}
	)
}


function getPackageMetadata(metadata_url, website_id, package_id, callback){
	request.post({
		url:metadata_url,
		body:JSON.stringify({}),
		headers:{'Content-Type': 'application/json'}
	},function(errMetadata, resMetadata, bodyMetadata){
		if(errMetadata){
			callback("Error cannot get "+website_id+"->"+package_id, null);
			return;
		}
		if(resMetadata.statusCode == 404){
			callback("404 Cannot find metadata "+website_id+"->"+package_id,null);
			return;
		}
		else if(resMetadata.statusCode == 403){
			callback("403 API key needed to access "+website_id+"->"+package_id,null);
			return;
		}
		try{
			var package_metadata=JSON.parse(bodyMetadata);
			//console.log(package_list.success);
		}
		catch(errParse){
			console.error(errParse);
			callback(website_id+"->"+package_id+" Error parsing response:"+bodyPackage,null)
		//	callback("Error parsing response:"+bodyPackage.help,null);
			return;
		}
		callback(null, package_metadata);
		return;
		
	})
}