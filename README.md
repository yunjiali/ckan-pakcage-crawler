ckan-pakcage-crawler
====================

ckan-package-crawler

* Get the base api url
* try: base_url/api, see if can get {version:1}
* If can't get anything, do it mannually.
* Get the package list
* try: GET base\_api\_url/action/package_list, see if can get a package list
* try: POST base\_api\_url/action/package_list with empty JSON object { }, see if can get a package list
* try: GET base\_api\_url/action/package_show?id=package\_id to get the metadata of the package
