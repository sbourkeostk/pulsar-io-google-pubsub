/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.ecosystem.io.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.Encoding;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Schema;
import com.google.pubsub.v1.SchemaSettings;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.ecosystem.io.pubsub.util.AvroUtils;
import org.apache.pulsar.ecosystem.io.pubsub.util.ProtobufUtils;


/**
 * PubsubPublisher wrapper Publisher of Google Cloud Pub/Sub.
 * <p>
 * Reference:
 * - https://cloud.google.com/pubsub/docs/samples/pubsub-create-avro-schema
 * - https://cloud.google.com/pubsub/docs/samples/pubsub-publish-avro-records#pubsub_publish_avro_records-java
 * - https://cloud.google.com/pubsub/docs/schemas#gcloud
 * </p>
 */
@Slf4j
public class PubsubPublisher {
    private final Publisher publisher;
    private final Topic topic;
    private final Object messageSchema;
    private final Schema.Type schemaType;

    private PubsubPublisher(Publisher publisher,
                            Topic topic,
                            Schema.Type schemaType, Object messageSchema) {
        this.publisher = publisher;
        this.topic = topic;
        this.schemaType = schemaType;
        this.messageSchema = messageSchema;
    }

    public static PubsubPublisher create(PubsubConnectorConfig config) throws Exception {
        TopicName topicName = TopicName.of(config.getPubsubProjectId(), config.getPubsubTopicId());

        TopicAdminSettings topicAdminSettings = TopicAdminSettings.newBuilder()
                .setTransportChannelProvider(config.getTransportChannelProvider())
                .setCredentialsProvider(config.getCredentialsProvider())
                .build();

        Topic topic;
        Schema schema = null;
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create(topicAdminSettings)) {
            try {
                topic = topicAdminClient.getTopic(topicName);
                schema = config.getOrCreateSchema();
            } catch (Exception ex) {
                if (ex instanceof NotFoundException) {
                    Topic.Builder topicBuilder = Topic.newBuilder();
                    topicBuilder.setName(topicName.toString());

                    if (!"".equals(config.getPubsubSchemaId())) {
                        schema = config.getOrCreateSchema();
                        SchemaSettings schemaSettings = SchemaSettings.newBuilder()
                                .setSchema(schema.getName())
                                .setEncoding(config.getPubsubSchemaEncoding())
                                .build();
                        topicBuilder.setSchemaSettings(schemaSettings);
                    }

                    topic = topicAdminClient.createTopic(topicBuilder.build());
                    log.info("{} topic created successfully", topicName);
                } else {
                    log.error("failed to create topic", ex);
                    throw ex;
                }
            }
        }

        String formattedSchema = topic.getSchemaSettings().getSchema();
        Object messageSchema = null;
        Schema.Type schemaType = null;

        if (!formattedSchema.contains("_deleted-schema_") && !"".equals(formattedSchema)) {
            if (schema == null) {
                throw new Exception("schema cannot be null, should be " + formattedSchema);
            }
            schemaType = schema.getType();
            if (schemaType == Schema.Type.AVRO) {
                messageSchema = AvroUtils.parseSchemaString(schema.getDefinition());
            } else if (schemaType == Schema.Type.PROTOCOL_BUFFER) {
                messageSchema = ProtobufUtils.parseSchemaString(schema.getDefinition());
            } else {
                throw new Exception("not supported scheme type " + schemaType);
            }
        }

        Publisher.Builder publishBuilder = Publisher.newBuilder(topicName)
                .setEndpoint(PubsubUtils.toEndpoint(config.getPubsubEndpoint()))
                .setChannelProvider(config.getTransportChannelProvider())
                .setCredentialsProvider(config.getCredentialsProvider());

        return new PubsubPublisher(publishBuilder.build(), topic, schemaType, messageSchema);
    }

    public static byte[] serializeObject(Object obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        return out.toByteArray();
    }

    public static Object deserializeByteArray(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return is.readObject();
    }

    public void send(Object data, ApiFutureCallback<String> callback) throws Exception {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        if (this.schemaType != null && this.messageSchema != null) {
            if (this.schemaType == Schema.Type.AVRO) {
                Encoder encoder;
                org.apache.avro.Schema schema = (org.apache.avro.Schema) this.messageSchema;
                Encoding encoding = this.topic.getSchemaSettings().getEncoding();
                switch (encoding) {
//                    case BINARY:  // TODO: BINARY not supported
//                        encoder = EncoderFactory.get().directBinaryEncoder(byteStream, null);
//                        encoder.writeBytes(data);
//                        encoder.flush();
//                        break;
                    case JSON:
                        encoder = EncoderFactory.get().jsonEncoder(schema, byteStream);
                        encoder.writeString((String) data);
                        encoder.flush();
                        break;
                    default:
                        throw new Exception("not support avro schema with " + encoding);
                }
            } else {
                throw new Exception("not support schema type: " + this.schemaType);
            }
        } else {
            // If the schema is not configured, we will convert this object to byte[]
            byteStream.write(serializeObject(data));
            byteStream.flush();
        }

        PubsubMessage message =
                PubsubMessage.newBuilder().setData(ByteString.copyFrom(byteStream.toByteArray())).build();
        ApiFuture<String> apiFuture = publisher.publish(message);
        if (callback != null) {
            ApiFutures.addCallback(apiFuture, callback, MoreExecutors.directExecutor());
        }
    }

    public void shutdown() throws InterruptedException {
        this.publisher.awaitTermination(3, TimeUnit.SECONDS);
        this.publisher.shutdown();
    }
}



