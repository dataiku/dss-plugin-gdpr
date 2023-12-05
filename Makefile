PLUGIN_VERSION=0.2.0
PLUGIN_ID=gdpr

all:
	cat plugin.json|json_pp > /dev/null
	ant clean
	ant
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip custom-fields java-lib java-policy-hooks python-runnables plugin.json
