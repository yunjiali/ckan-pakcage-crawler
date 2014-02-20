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
var winston = require('winston')

if(argv.help){
	console.info("--id		The ID of the website that you want to get package metadata, such as 'data_gov_uk'.");
	console.info("--all		Get the package metadata for all the websites in the database.");
	return;
	process.exit(0);
}

var logger = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)({ level: 'info' }),
      new (winston.transports.File)({ level:'info',filename: 'package-metadata.log' })
    ]
  });

var mongourl = "";
if(CONFIG.dbUsername && CONFIG.dbPassword)
	mongourl=CONFIG.dbUsername+":"+CONFIG.dbPassword+"@";
mongourl+=CONFIG.dbHost+":"+CONFIG.dbPort+"/"+CONFIG.dbName;
var db = monk(mongourl);
logger.info("Connected to MongoDB.");
logger.info("Create Index.");
//db.get('instances').ensureIndex({website_id:1},{unique:true});
db.get('packages').ensureIndex({website_id:1, package_id:1}); 


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
			
			logger.info(argv.id+" has "+packageArray.length+" packages.");
			async.eachSeries(packageArray,function(p,packageCallback){
				getPackageMetadata(p.metadata_url, p.website_id,p.package_id,
					function(errMetadata, package_obj){
						if(errMetadata){
							logger.error(p.website_id+"->"+p.package_id+" error:"+errMetadata);
							packageCallback(null);
							return;
						}
						else{
							if(!package_obj.success){
								logger.error("Error get package metadata "+p.website_id+"->"+p.package_id);
								packageCallback(null);
								return;
							}else{
								var metadata_obj = {};
								metadata_obj.website_id = p.website_id;
								metadata_obj.package_id = p.package_id;
								metadata_obj.metadata_url = p.metadata_url;
								metadata_obj.metadata = package_obj.result;
								db.get("packages").insert(metadata_obj, function(errInsert, metadata_new){
									if(errInsert){
										logger.error("Error insert package metadata "+p.website_id+"->"+p.package_id);
										packageCallback(null);
										return;
									}
									else
									{
										logger.info("Successfully insert package metadata "+p.website_id+"->"+p.package_id)
										packageCallback(null, metadata_new);
									}
								});
							}
						}
					});
			},function(errPackages,results){
				if(errPackages){
					logger.error("Error "+errPackages)
					process.exit(1);
				}
				else{
					logger.info("Finish.");
					process.exit(0);
				}
			});
		}
	)
}
else if(argv.all){
	db.get("instances").find({package_list:{$exists:true}},
		{id:true,api_base_url:true,package_list:true},function(errInstances, instances){
			logger.info(instances.length+" instances found.");
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
			logger.info(packageArray.length+" packages in all.");

			async.eachSeries(packageArray,function(p,packageCallback){
				getPackageMetadata(p.metadata_url, p.website_id,p.package_id,
					function(errMetadata, package_obj){
						if(errMetadata){
							logger.error(p.website_id+"->"+p.package_id+" error:"+errMetadata);
							packageCallback(null);
							return;
						}
						else{
							if(!package_obj.success){
								logger.error("Error get package metadata "+p.website_id+"->"+p.package_id);
								packageCallback(null);
								return;
							}else{
								var metadata_obj = {};
								metadata_obj.website_id = p.website_id;
								metadata_obj.package_id = p.package_id;
								metadata_obj.metadata_url = p.metadata_url;
								metadata_obj.metadata = package_obj.result;
								db.get("packages").insert(metadata_obj, function(errInsert, metadata_new){
									if(errInsert){
										logger.error("Error insert package metadata "+p.website_id+"->"+p.package_id);
										packageCallback(null);
										return;
									}
									else
									{
										logger.info("Successfully insert package metadata "+p.website_id+"->"+p.package_id)
										packageCallback(null, metadata_new);
									}
								});
							}
						}
					});
			},function(errPackages,results){
				if(errPackages){
					logger.error("Error "+errPackages)
					process.exit(1);
				}
				else{
					logger.info("Finish.");
					process.exit(0);
				}
			});

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
		else if(resMetadata.statusCode == 500){
			callback("500 Server error to access "+website_id+"->"+package_id,null);
			return;
		}
		try{
			var package_metadata=JSON.parse(bodyMetadata);
			//logger.log(package_list.success);
		}
		catch(errParse){
			logger.error(errParse);
			callback(website_id+"->"+package_id+" Error parsing response:"+bodyMetadata,null)
		//	callback("Error parsing response:"+bodyPackage.help,null);
			return;
		}
		callback(null, package_metadata);
		return;
		
	})
}