/*
Copyright 2020 Splunk Inc. 

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
const splunk = require('../helpers/splunk');
const HalliganLogger = require('../helpers/halligan');

const halliganLogger = new HalliganLogger({
    url: process.env['HALLIGAN_SINK_URL'],
    cluster: process.env['CLUSTER_NAME'],
    provider: 'aro',
    environment: process.env['ENVIRONMENT'],
    region: process.env['REGION'],
});

module.exports = async function (context, eventHubMessages) {
    const sourcetype = process.env["DIAGNOSTIC_LOG_SOURCETYPE"];

    for (const event of eventHubMessages) {
        await splunk
                .sendToHEC(event, sourcetype)
                .catch(err => {
                    context.log.error(`Error posting to Splunk HTTP Event Collector: ${err}`);

                    // If the event was not successfully sent to Splunk, drop the event in a storage blob
                    context.bindings.outputBlob = event;
                });

        if (sourcetype === 'k8s:audit' && halliganLogger.enabled) {
            try {
                let parsed = event;
                if (typeof event === 'string') {
                    try { parsed = JSON.parse(event); } catch (_) {}
                }
                const records = (parsed && parsed.records) ? parsed.records : [parsed];
                for (const record of records) {
                    if (halliganLogger.isDORARelevant(record)) {
                        halliganLogger.logEvent(record);
                    }
                }
            } catch (e) {
                context.log.error(`Halligan filter error: ${e.message}`);
            }
        }
    }

    await halliganLogger.flushAsync();

    context.done();
};