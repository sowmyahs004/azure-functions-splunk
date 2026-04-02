/*
Copyright 2022 Splunk Inc.

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
const { BlobServiceClient } = require('@azure/storage-blob');

module.exports = async function (context, eventGridEvent) {
    const blobUrl = eventGridEvent.data.url;
    context.log(`Processing blob: ${blobUrl}`);

    // Parse container and blob name from URL
    // URL format: https://<account>.blob.core.windows.net/<container>/<blobPath>
    const url = new URL(blobUrl);
    const pathParts = url.pathname.split('/').filter(p => p);
    const blobContainer = pathParts[0];
    const blobName = pathParts.slice(1).join('/');

    // Only process blobs from the configured BLOB_PATH container
    const expectedContainer = process.env["BLOB_PATH"];
    if (blobContainer !== expectedContainer) {
        context.log(`Skipping blob from container ${blobContainer}, expected ${expectedContainer}`);
        context.done();
        return;
    }

    const blobServiceClient = BlobServiceClient.fromConnectionString(process.env["BLOB_CONNECTION_STRING"]);
    const containerClient = blobServiceClient.getContainerClient(blobContainer);
    const blobClient = containerClient.getBlobClient(blobName);

    const downloadResponse = await blobClient.download(0);
    const chunks = [];
    for await (const chunk of downloadResponse.readableStreamBody) {
        chunks.push(chunk);
    }
    const blobContent = JSON.parse(Buffer.concat(chunks).toString('utf-8'));

    await splunk
            .sendToHEC(blobContent)
            .catch(err => {
                context.log.error(`Error posting to Splunk HTTP Event Collector: ${err}`);

                // If the event was not successfully sent to Splunk, drop the event in a storage blob container undeliverable-nsg-events
                context.bindings.outputBlob = JSON.stringify(blobContent);
            });
    context.done();
};
