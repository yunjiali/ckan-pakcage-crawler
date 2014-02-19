//write ckan instances http://instances.ckan.org/config/instances.json into database
//Attach "api" at the back of instance.url and see if the standard api url exists
//if not, we need to find it mannually
//We will also check if the website is active, if not, we need to ignore it

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


var mongourl = "";
if(CONFIG.dbUsername && CONFIG.dbPassword)
	mongourl=CONFIG.dbUsername+":"+CONFIG.dbPassword+"@";
mongourl+=CONFIG.dbHost+":"+CONFIG.dbPort+"/"+CONFIG.dbName;
var db = monk(mongourl);
console.info("Connected to MongoDB.");
console.info("Create Index.");
db.get('instances').ensureIndex({id:1},{unique:true});
db.get('instances').ensureIndex({url:1, title:1, description:1, locale:1, "facets.key":1, "facets.value":1}); 

//read the list of api-base-urls.json
var file = 'api-base-urls.json';
var apiBaseList = jf.readFileSync(file);
request.get({
	url:CONFIG.instancesUrl
},function(errInstances, resInstances, bodyInstances){
	var instancesJSON = JSON.parse(bodyInstances);
	//console.log(instancesJSON.length);
	async.eachSeries(instancesJSON, function(instance, instanceCallback){
		//Filter EU sites?
		//test the instances to get the base api url
		var apiUrl = S(instance.url).ensureRight('/').s+"api";
		request.get({
			url:apiUrl
		},function(errApiUrl, resApiUrl, bodyApiUrl){
			if(errApiUrl){
				console.error("Request API error:"+errApiUrl);
				instance.active = false; //error opening the website
				instance.ckanApi = false;
				instance.api_base_url = null;
			}
			else if(resApiUrl.statusCode == 200){ //standard api
				if(instance.id=="data_qld_gov_au"){
					//austrilian port is a little bit different
					instance.api_base_url="https://data.qld.gov.au/api"
				}else if(instance.id=="opendata_aragon_es"){
					instance.api_base_url="http://opendata.aragon.es/catalogo/api"
				}else{
					instance.api_base_url = apiUrl;
				}
				

				instance.ckanApi = true;
				instance.active = true;
			}
			else{ //statusCode == 400

				//Check the table
				//Another useful resource: https://github.com/knudmoeller/open_data_licences/blob/master/catalog_apis.csv
				//and https://github.com/okfn/ckan-instances/issues/60
				var baseUrlObj = _.findWhere(apiBaseList,{id:instance.id});
				if(baseUrlObj){
					instance.api_base_url = baseUrlObj.apiBaseUrl;
					instance.ckanApi = baseUrlObj.ckanApi;
					instance.active = true;
				}
				else{
					instance.api_base_url = null;
					instance.ckanApi = false;
					instance.active = true;
				}
			}

			instance.authentication = true;//by default, all the API needs authentication
			instance.apiKey = null; //by default, all the API key are null
			db.get("instances").insert(instance, function(errInsert, ins){
				if(errInsert){
					console.error("Insert error:"+errInsert);
					console.error(JSON.stringify(instance, null, 4));
					instanceCallback(errInsert);
				}
				else{
					console.info("Add instance "+instance.id+", api url "+instance.api_base_url);
					instanceCallback(null);
				}
			});
		});
	}, function(errInstances, results){
		if(errInstances)
			process.exit(1);

		console.log("Finished.");
		process.exit(0);
	});
})