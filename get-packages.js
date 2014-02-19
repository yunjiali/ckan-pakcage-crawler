//get the list of packages through CKAN API
//The standard request should be:
//POST: api_base_url/action/package_list
//If we cannot get anything from package_list action, we have to check it mannually

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
	console.info("--id		The ID of the website, such as 'data_gov_uk'.");
	console.info("--all		Get the package_list for all the websites in the database.");
	return;
	process.exit(0);
}

var mongourl = "";
if(CONFIG.dbUsername && CONFIG.dbPassword)
	mongourl=CONFIG.dbUsername+":"+CONFIG.dbPassword+"@";
mongourl+=CONFIG.dbHost+":"+CONFIG.dbPort+"/"+CONFIG.dbName;
var db = monk(mongourl);
console.info("Connected to MongoDB.");

if(argv.id){
	db.get("instances").findOne({id:argv.id},function(errFind, instance){
		if(instance===undefined){
			console.error("Cannot find website with id "+argv.id);
			process.exit(1);
			return;
		}

		//console.log(JSON.stringify(instance));

		if(instance.api_base_url == null || instance.ckanApi == false){
			console.error(argv.id+" doesn't follow CKAN API. No data in the db is updated.");
			process.exit(1);
			return;
		}

		getPackageList(instance, function(errPackageList, package_list_obj){
			if(errPackageList){
				console.error(errPackageList);
				process.exit(1);
				return;
			}
			else{
				console.log(package_list_obj.success);
				if(!package_list_obj.success){
					console.error("Error get package_list:"+package_list_obj.help);
					process.exit(1);
					return;
				}else{
					var package_list = package_list_obj.result;
					console.log("Get "+package_list.length+" packages.")
					db.get("instances").findAndModify({
						query:{id:instance.id},
						update:{$set:{package_list:package_list, last_updated:Date.now(), authentication:false}},
						upsert:true
					},function(errUpdate, updatedIns){
						if(errUpdate){
							console.error("Error update package_list:"+errUpdate);
							process.exit(1);
							return;
						}

						console.info("Updated the package list for instance "+argv.id);
						process.exit(0);
					});
				}
			}
		});
	});
}
else if(argv.all){
	db.get("instances").find({},function(errFind, instances){
		async.eachSeries(instances,function(instance, instanceCallback){
			if(instance===undefined){
				console.error("Cannot find website with id "+instance.id);
				instanceCallback("Cannot find website with id "+instance.id);
			}

			//console.log(JSON.stringify(instance));

			if(instance.api_base_url == null || instance.ckanApi == false){
				console.info(instance.id+" doesn't follow CKAN API. No data in the db is updated.");
				instanceCallback(null);
				return;
			}

			getPackageList(instance, function(errPackageList, package_list_obj){
				if(errPackageList){
					console.error(errPackageList);
					instanceCallback(null);
					return;
				}
				else{
					//console.log("success:"+package_list_obj.success);
					if(!package_list_obj.success){
						console.error("Error get package_list:"+package_list_obj.help);
						instanceCallback(null);
						return;
					}else{
						var package_list = package_list_obj.result;
						console.log("Get "+package_list.length+" packages.")
						db.get("instances").findAndModify({
							query:{id:instance.id},
							update:{$set:{package_list:package_list, last_updated:Date.now(), authentication:false}},
							upsert:true
						},function(errUpdate, updatedIns){
							if(errUpdate){
								console.error("Error update package_list:"+errUpdate);
								instanceCallback("Error update package_list:"+errUpdate);
							}
							instanceCallback(null, instance);
						});
					}
				}
			});
		},function(errIntances, results){
			if(errIntances){
				console.log("Finished with errors.")
			}else{
				console.log("Finished.")
			}
		})
	});
}

function getPackageList(instance, callback){
	var api_base_url = instance.api_base_url;
	var package_list_url = api_base_url+"/action/package_list";
	request.post({
		url:package_list_url,
		body:JSON.stringify({}), //empty json object, get all packages
		headers:{'Content-Type': 'application/json'}
	},function(errPackage, resPackage, bodyPackage){
		if(errPackage){
			callback("Error "+instance.id+" "+errPackage, null);
			return;
		}
		if(resPackage.statusCode == 404){
			callback(instance.id+" Cannot find the package_list api.",null);
			return;
		}
		else if(resPackage.statusCode == 403){
			callback(instance.id+ " API key needed to access the package list",null);
			return;
		}
		try{
			var package_list=JSON.parse(bodyPackage);
			//console.log(package_list.success);
		}
		catch(errParse){
			console.error(errParse);
			callback(instance.id+ " Error parsing response:"+bodyPackage,null)
		//	callback("Error parsing response:"+bodyPackage.help,null);
			return;
		}
		callback(null, package_list);
		return;
		
	})
}

