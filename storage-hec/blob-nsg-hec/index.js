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
const { chain } = require('stream-chain');
const { parser } = require('stream-json');
const Pick = require('stream-json/filters/Pick');
const { streamArray } = require('stream-json/streamers/StreamArray');

const BATCH_SIZE = 200;

module.exports = async function (context, eventGridEvent) {
    const blobUrl = eventGridEvent.data.url;
    context.log(`Processing blob: ${blobUrl}`);

    const url = new URL(blobUrl);
    const pathParts = url.pathname.split('/').filter(p => p);
    const blobContainer = pathParts[0];
    const blobName = pathParts.slice(1).join('/');

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

    // Stream the blob and process records in batches to avoid OOM on large blobs (up to 1GB)
    await new Promise((resolve, reject) => {
        let batch = [];

        const pipeline = chain([
            downloadResponse.readableStreamBody,
            parser(),
            Pick.withParser({ filter: 'records' }),
            streamArray()
        ]);

        pipeline.on('data', async ({ value: record }) => {
            batch.push(record);

            if (batch.length >= BATCH_SIZE) {
                pipeline.pause();
                const currentBatch = batch.splice(0, BATCH_SIZE);
                await splunk.sendToHEC({ records: currentBatch })
                    .catch(err => {
                        context.log.error(`Error posting batch to Splunk: ${err}`);
                    });
                pipeline.resume();
            }
        });

        pipeline.on('end', async () => {
            if (batch.length > 0) {
                await splunk.sendToHEC({ records: batch })
                    .catch(err => {
                        context.log.error(`Error posting final batch to Splunk: ${err}`);
                    });
            }
            resolve();
        });

        pipeline.on('error', reject);
    });

    context.done();
};
