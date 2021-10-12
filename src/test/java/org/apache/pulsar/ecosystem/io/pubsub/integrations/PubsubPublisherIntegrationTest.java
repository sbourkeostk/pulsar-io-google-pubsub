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
package org.apache.pulsar.ecosystem.io.pubsub.integrations;

import com.google.api.core.ApiFutureCallback;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.ecosystem.io.pubsub.PubsubConnectorConfig;
import org.apache.pulsar.ecosystem.io.pubsub.PubsubPublisher;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link PubsubPublisher}.
 */
public class PubsubPublisherIntegrationTest {
    @Test
    public void testCreatePublisher() throws Exception {
        String projectId = "pulsar-io-google-pubsub";
        String topicId = "test-pubsub-" + System.currentTimeMillis();
        String credential = "";
        String endpoint = "localhost:8085";

        Map<String, Object> properties = new HashMap<>();
        properties.put("pubsubEndpoint", endpoint);
        properties.put("pubsubProjectId", projectId);
        properties.put("pubsubCredential", credential);
        properties.put("pubsubTopicId", topicId);

        PubsubConnectorConfig config = PubsubConnectorConfig.load(properties);

        PubsubPublisher pubsubPublisher = PubsubPublisher.create(config);
        pubsubPublisher.send("hello".getBytes(StandardCharsets.UTF_8), new ApiFutureCallback<String>() {
            @Override
            public void onFailure(Throwable throwable) {
                Assert.fail(throwable.getMessage());
            }

            @Override
            public void onSuccess(String s) {

            }
        });
        pubsubPublisher.shutdown();
    }

    @Test
    public void testCreatePublisherWithSchema() throws Exception {
        String projectId = "pulsar-io-google-pubsub";
        String topicId = "test-pubsub-topic-" + System.currentTimeMillis();
        String credential = "";
        String endpoint = "localhost:8085";

        String schemaId = "test-pubsub-schema-" + System.currentTimeMillis();
        String schemaType = "AVRO";
        String schemaEncoding = "JSON";
        String schemaDefinition = "{\n"
                + " \"type\" : \"record\",\n"
                + " \"name\" : \"User\",\n"
                + " \"fields\" : [\n"
                + "   {\n"
                + "     \"name\" : \"key\",\n"
                + "     \"type\" : \"string\"\n"
                + "   }\n"
                + " ]\n"
                + "}";

        Map<String, Object> properties = new HashMap<>();
        properties.put("pubsubEndpoint", endpoint);
        properties.put("pubsubProjectId", projectId);
        properties.put("pubsubCredential", credential);
        properties.put("pubsubTopicId", topicId);
        properties.put("pubsubSchemaId", schemaId);
        properties.put("pubsubSchemaType", schemaType);
        properties.put("pubsubSchemaEncoding", schemaEncoding);
        properties.put("pubsubSchemaDefinition", schemaDefinition);

        PubsubConnectorConfig config = PubsubConnectorConfig.load(properties);
        PubsubPublisher pubsubPublisher = PubsubPublisher.create(config);
        pubsubPublisher.send("{\"key\":\"hello\"}", new ApiFutureCallback<String>() {
            @Override
            public void onFailure(Throwable throwable) {
                Assert.fail(throwable.getMessage());
            }

            @Override
            public void onSuccess(String s) {

            }
        });
        pubsubPublisher.shutdown();
    }
}
