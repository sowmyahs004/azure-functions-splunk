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

const { BlobServiceClient } = require('@azure/storage-blob');
const axios = require('axios');

const STATE_CONTAINER = 'aro-hec-state';
const CONTAINERS_PREFIX = 'insights-logs-';
const BATCH_SIZE = 200;
const CHUNK_SIZE = 10 * 1024 * 1024; // 10MB — keeps memory safe even for 900MB blobs
const LOOKBACK_MS = 2 * 60 * 60 * 1000; // only scan blobs modified in last 2 hours

module.exports = async function (context) {
    const connectionString = process.env['BLOB_CONNECTION_STRING'];
    const hecUrl = process.env['SPLUNK_HEC_URL'];
    const hecToken = process.env['SPLUNK_HEC_TOKEN'];

    const blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
    const stateContainerClient = blobServiceClient.getContainerClient(STATE_CONTAINER);
    await stateContainerClient.createIfNotExists();

    for await (const container of blobServiceClient.listContainers({ prefix: CONTAINERS_PREFIX })) {
        try {
            await processContainer(context, blobServiceClient, stateContainerClient, container.name, hecUrl, hecToken);
        } catch (err) {
            context.log.error(`Error processing container ${container.name}: ${err.message}`);
        }
    }
};

async function processContainer(context, blobServiceClient, stateContainerClient, containerName, hecUrl, hecToken) {
    const containerClient = blobServiceClient.getContainerClient(containerName);
    // Derive sourcetype from container name: insights-logs-kube-audit -> azure:aro:kube-audit
    const sourcetype = `azure:aro:${containerName.replace('insights-logs-', '')}`;
    const stateKey = `${containerName}.json`;

    let state = await loadState(stateContainerClient, stateKey);
    state = cleanOldState(state);

    const cutoff = new Date(Date.now() - LOOKBACK_MS);

    for await (const blobItem of containerClient.listBlobsFlat()) {
        if (!blobItem.name.endsWith('PT1H.json')) continue;
        if (new Date(blobItem.properties.lastModified) < cutoff) continue;

        const lastOffset = state[blobItem.name] || 0;
        const blobSize = blobItem.properties.contentLength;
        if (blobSize <= lastOffset) continue; // no new data

        try {
            const newOffset = await processBlobRange(
                context, containerClient, blobItem.name,
                lastOffset, blobSize, sourcetype, hecUrl, hecToken
            );
            state[blobItem.name] = newOffset;
            context.log(`${containerName}/${blobItem.name}: processed bytes ${lastOffset}–${newOffset}`);
        } catch (err) {
            context.log.error(`Error processing ${containerName}/${blobItem.name}: ${err.message}`);
        }
    }

    await saveState(stateContainerClient, stateKey, state);
}

async function processBlobRange(context, containerClient, blobName, startOffset, endOffset, sourcetype, hecUrl, hecToken) {
    const blobClient = containerClient.getBlobClient(blobName);
    let currentOffset = startOffset;
    let leftover = ''; // incomplete line carried across chunk boundaries
    let anyComplete = false;

    while (currentOffset < endOffset) {
        const chunkSize = Math.min(CHUNK_SIZE, endOffset - currentOffset);
        const downloadResponse = await blobClient.download(currentOffset, chunkSize);
        const buffer = await streamToBuffer(downloadResponse.readableStreamBody);
        currentOffset += chunkSize;

        const text = leftover + buffer.toString('utf8');
        const lastNewline = text.lastIndexOf('\n');

        if (lastNewline === -1) {
            leftover = text;
            continue;
        }

        anyComplete = true;
        const completeLines = text.substring(0, lastNewline);
        leftover = text.substring(lastNewline + 1);

        const lines = completeLines.split('\n').filter(l => l.trim().length > 0);
        for (let i = 0; i < lines.length; i += BATCH_SIZE) {
            await sendBatchToHEC(lines.slice(i, i + BATCH_SIZE), sourcetype, hecUrl, hecToken);
        }
    }

    if (!anyComplete) return startOffset;
    // Don't advance past incomplete last line — re-read it next invocation
    return endOffset - Buffer.byteLength(leftover, 'utf8');
}

async function sendBatchToHEC(lines, sourcetype, hecUrl, hecToken) {
    let payload = '';
    for (const line of lines) {
        try {
            const parsed = JSON.parse(line);
            const hecEvent = { event: parsed, sourcetype };
            if (parsed.time) {
                hecEvent.time = Math.floor(new Date(parsed.time).getTime() / 1000);
            }
            payload += JSON.stringify(hecEvent);
        } catch {
            // skip malformed NDJSON lines
        }
    }
    if (!payload) return;

    await axios.post(hecUrl, payload, {
        headers: { Authorization: `Splunk ${hecToken}` }
    });
}

async function loadState(stateContainerClient, stateKey) {
    try {
        const blobClient = stateContainerClient.getBlobClient(stateKey);
        const download = await blobClient.download();
        const content = await streamToBuffer(download.readableStreamBody);
        return JSON.parse(content.toString('utf8'));
    } catch {
        return {};
    }
}

async function saveState(stateContainerClient, stateKey, state) {
    const blockBlobClient = stateContainerClient.getBlockBlobClient(stateKey);
    const content = JSON.stringify(state);
    await blockBlobClient.upload(content, Buffer.byteLength(content), { overwrite: true });
}

// Remove state entries for blobs older than 48 hours to prevent unbounded growth
function cleanOldState(state) {
    const maxAgeMs = 48 * 60 * 60 * 1000;
    const now = Date.now();
    for (const blobName of Object.keys(state)) {
        const match = blobName.match(/y=(\d+)\/m=(\d+)\/d=(\d+)\/h=(\d+)/);
        if (match) {
            const blobDate = new Date(`${match[1]}-${match[2]}-${match[3]}T${match[4]}:00:00Z`);
            if (now - blobDate.getTime() > maxAgeMs) {
                delete state[blobName];
            }
        }
    }
    return state;
}

async function streamToBuffer(readableStream) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        readableStream.on('data', data => chunks.push(Buffer.isBuffer(data) ? data : Buffer.from(data)));
        readableStream.on('end', () => resolve(Buffer.concat(chunks)));
        readableStream.on('error', reject);
    });
}
