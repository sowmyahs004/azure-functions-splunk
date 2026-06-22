'use strict';

const https = require('https');
const http = require('http');

/**
 * Halligan logger for DORA-relevant K8s audit events.
 *
 * Filters audit events to only forward deploy-related mutations
 * (Deployments, StatefulSets in ns-team-* namespaces) to a Halligan/Hoolihan
 * Kafka endpoint. Fire-and-forget: errors never affect Splunk delivery.
 *
 * The sink URL (which embeds the auth token) is injected via the
 * HALLIGAN_SINK_URL environment variable, set by the post-sync hook in
 * ethos-core-aks-control-plane-logging from the halligan ExternalSecret.
 *
 * Environment variables:
 *   HALLIGAN_SINK_URL  - Full Halligan endpoint URL (contains auth token)
 *   ENVIRONMENT        - e.g. "prod", "stage"
 *   REGION             - e.g. "nld2", "va7"
 *   CLUSTER_NAME       - Azure cluster name
 */

const DORA_RESOURCES = new Set(['deployments', 'statefulsets']);
const DORA_VERBS = new Set(['create', 'patch']);

function HalliganLogger(config) {
    this.url = config.url || '';
    this.cluster = config.cluster || '';
    this.provider = config.provider || 'aro';
    this.environment = config.environment || '';
    this.region = config.region || '';
    this.enabled = !!this.url;
    this.payloads = [];
}

HalliganLogger.prototype.isDORARelevant = function (auditEvent) {
    try {
        return auditEvent.stage === 'ResponseComplete'
            && DORA_VERBS.has(auditEvent.verb)
            && auditEvent.objectRef
            && DORA_RESOURCES.has(auditEvent.objectRef.resource)
            && auditEvent.objectRef.namespace
            && auditEvent.objectRef.namespace.startsWith('ns-team-')
            && auditEvent.responseStatus
            && auditEvent.responseStatus.code === 200;
    } catch (e) {
        return false;
    }
};

HalliganLogger.prototype.trimAuditEvent = function (auditEvent) {
    var trimmed = {
        kind: auditEvent.kind,
        apiVersion: auditEvent.apiVersion,
        auditID: auditEvent.auditID,
        stage: auditEvent.stage,
        verb: auditEvent.verb,
        requestReceivedTimestamp: auditEvent.requestReceivedTimestamp,
        stageTimestamp: auditEvent.stageTimestamp,
        user: {
            username: (auditEvent.user || {}).username,
            groups: (auditEvent.user || {}).groups,
        },
        objectRef: auditEvent.objectRef,
        responseStatus: auditEvent.responseStatus,
    };

    var resp = auditEvent.responseObject;
    if (resp) {
        var meta = resp.metadata || {};
        trimmed.responseObject = {
            kind: resp.kind,
            apiVersion: resp.apiVersion,
            metadata: {
                name: meta.name,
                namespace: meta.namespace,
                uid: meta.uid,
                generation: meta.generation,
                creationTimestamp: meta.creationTimestamp,
                labels: meta.labels,
                annotations: {
                    'deployment.kubernetes.io/revision': (meta.annotations || {})['deployment.kubernetes.io/revision'],
                },
            },
            spec: {
                replicas: (resp.spec || {}).replicas,
                template: {
                    spec: {
                        containers: ((((resp.spec || {}).template || {}).spec || {}).containers || []).map(function (c) {
                            return { name: c.name, image: c.image };
                        }),
                        initContainers: ((((resp.spec || {}).template || {}).spec || {}).initContainers || []).map(function (c) {
                            return { name: c.name, image: c.image };
                        }),
                    },
                },
            },
        };
        if (trimmed.responseObject.spec.template.spec.initContainers.length === 0) {
            delete trimmed.responseObject.spec.template.spec.initContainers;
        }
    }

    return trimmed;
};

HalliganLogger.prototype.logEvent = function (auditEvent) {
    if (!this.enabled) return;
    this.payloads.push({
        message: this.trimAuditEvent(auditEvent),
        ethos_cluster: this.cluster,
        environment: this.environment,
        region: this.region,
        provider: this.provider,
    });
};

/**
 * Flush all collected events to Halligan in a single batch POST.
 * Always resolves — never rejects. Splunk delivery is never affected.
 */
HalliganLogger.prototype.flushAsync = function () {
    var self = this;

    if (!this.enabled || this.payloads.length === 0) {
        return Promise.resolve(0);
    }

    var count = this.payloads.length;
    var body = JSON.stringify(this.payloads.map(function (payload, i) {
        return {
            id: 'audit-' + Date.now() + '-' + i,
            data: JSON.stringify(payload),
        };
    }));
    this.payloads.length = 0;

    var parsedUrl = new URL(this.url);
    var requester = parsedUrl.protocol === 'https:' ? https : http;

    return new Promise(function (resolve) {
        var req = requester.request({
            hostname: parsedUrl.hostname,
            port: parsedUrl.port,
            path: parsedUrl.pathname + parsedUrl.search,
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-gw-ims-client-id': 'hoolihanService1',
                'user-agent': 'dora-audit-azure/1.0',
            },
            rejectUnauthorized: false,
        }, function (res) {
            var data = '';
            res.on('data', function (chunk) { data += chunk; });
            res.on('end', function () {
                if (res.statusCode >= 200 && res.statusCode < 300) {
                    console.log('Halligan: sent ' + count + ' DORA event(s)');
                } else {
                    console.error('Halligan: HTTP ' + res.statusCode + ' - ' + data);
                }
                resolve(count);
            });
        });

        req.on('error', function (err) {
            console.error('Halligan: POST failed - ' + err.message);
            resolve(0);
        });

        req.end(body);
    });
};

module.exports = HalliganLogger;
