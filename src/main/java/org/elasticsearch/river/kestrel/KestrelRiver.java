/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.river.kestrel;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import iinteractive.kestrel.Client;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class KestrelRiver extends AbstractRiverComponent implements River {

    private final Client client;

    private final String kestrelHost;
    private final int kestrelPort;

    private final String kestrelQueue;

    private final int bulkSize;
    private final TimeValue bulkTimeout;
    private final boolean ordered;

    private volatile boolean closed = false;

    private volatile Thread thread;

    private volatile ConnectionFactory connectionFactory;

    @SuppressWarnings({"unchecked"})
    @Inject public KestrelRiver(RiverName riverName, RiverSettings settings, Client client) {
        super(riverName, settings);
        this.client = client;

        if (settings.settings().containsKey("kestrel")) {
            Map<String, Object> kestrelSettings = (Map<String, Object>) settings.settings().get("kestrel");
            kestrelHost = XContentMapValues.nodeStringValue(kestrelSettings.get("host"), ConnectionFactory.DEFAULT_HOST);
            kestrelPort = XContentMapValues.nodeIntegerValue(kestrelSettings.get("port"), ConnectionFactory.DEFAULT_AMQP_PORT);


            kestrelQueue = XContentMapValues.nodeStringValue(kestrelSettings.get("queue"), "elasticsearch");
        } else {
            kestrelHost = ConnectionFactory.DEFAULT_HOST;
            kestrelPort = ConnectionFactory.DEFAULT_AMQP_PORT;

            kestrelQueue = "elasticsearch";
        }

        if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            if (indexSettings.containsKey("bulk_timeout")) {
                bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "10ms"), TimeValue.timeValueMillis(10));
            } else {
                bulkTimeout = TimeValue.timeValueMillis(10);
            }
            ordered = XContentMapValues.nodeBooleanValue(indexSettings.get("ordered"), false);
        } else {
            bulkSize = 100;
            bulkTimeout = TimeValue.timeValueMillis(10);
            ordered = false;
        }
    }

    @Override public void start() {
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(kestrelHost);
        connectionFactory.setPort(kestrelPort);

        logger.info("creating kestrel river, host [{}], port [{}]", connectionFactory.getHost(), connectionFactory.getPort());

        thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "kestrel_river").newThread(new Consumer());
        thread.start();
    }

    @Override public void close() {
        if (closed) {
            return;
        }
        logger.info("closing kestrel river");
        closed = true;
        thread.interrupt();
    }

    private class Consumer implements Runnable {

        private Client client;

        @Override public void run() {
            while (true) {
                if (closed) {
                    return;
                }
                try {
                    client = Client(kestrelHost, kestrelPort);
                    client.connect();
                } catch (Exception e) {
                    if (!closed) {
                        logger.warn("failed to created a connection", e);
                    } else {
                        continue;
                    }
                    cleanup(0, "failed to connect");
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        // ignore, if we are closing, we will exit later
                    }
                }

                // now use the queue to listen for messages
                while (true) {
                    if (closed) {
                        break;
                    }
                    try {
                        // What does ES use for JSON?
                        JSONObject message = client.get(kestrelQueue, XXX);
                    } catch (Exception e) {
                        if (!closed) {
                            logger.error("failed to get next message, reconnecting...", e);
                        }
                        cleanup(0, "failed to get message");
                        break;
                    }

                    if (task != null && task.getBody() != null) {
                        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

                        try {
                            // XXX
                            bulkRequestBuilder.add(task.getBody(), 0, task.getBody().length, false);
                        } catch (Exception e) {
                            logger.warn("failed to parse request for delivery tag [{}], ack'ing...", e, task.getEnvelope().getDeliveryTag());
                            // Supposed to fail an item here…
                            continue;
                        }

                        // We got one message so far
                        int messageCount = 1;

                        if (bulkRequestBuilder.numberOfActions() < bulkSize) {
                            // try and spin some more of those without timeout, so we have a bigger bulk (bounded by the bulk size)
                            try {
                                while ((message = client.get(kestrelQueue, bulkTimeout.millis())) != null) {
                                    try {
                                        bulkRequestBuilder.add(message.getBody(), 0, message.getBody().length, false);
                                    } catch (Exception e) {
                                        // Supposed to fail an item here…
                                    }
                                    messageCount++; // Got another message
                                    if (bulkRequestBuilder.numberOfActions() >= bulkSize) {
                                        break;
                                    }
                                }
                            } catch (InterruptedException e) {
                                if (closed) {
                                    break;
                                }
                            }
                        }

                        if (logger.isTraceEnabled()) {
                            logger.trace("executing bulk with [{}] actions", bulkRequestBuilder.numberOfActions());
                        }

                        try {
                            BulkResponse response = bulkRequestBuilder.execute().actionGet();
                            if (response.hasFailures()) {
                                // TODO write to exception queue?
                                logger.warn("failed to execute" + response.buildFailureMessage());
                            }
                            // Confirm everything we snatched out of the queue
                            try {
                                client.confirm(kestrelQueue, messageCount);
                            } catch (Exception e1) {
                                logger.warn("failed to ack", e1);
                            }
                        } catch (Exception e) {
                            logger.warn("failed to execute bulk", e);
                        }
                    }
                }
            }
        }

        private void cleanup(int code, String message) {
            try {
                client.disconnect();
            } catch (Exception e) {
                logger.debug("failed to disconnect", e);
            }
        }
    }
}
