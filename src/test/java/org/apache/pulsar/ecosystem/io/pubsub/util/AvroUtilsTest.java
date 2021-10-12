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
package org.apache.pulsar.ecosystem.io.pubsub.util;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link AvroUtils}.
 */
public class AvroUtilsTest {
    @Test
    public void testParseSchemaString() {
        final String avroSchemaString = "{\n"
                + " \"type\" : \"record\",\n"
                + " \"name\" : \"Avro\",\n"
                + " \"fields\" : [\n"
                + "   {\n"
                + "     \"name\" : \"StringField\",\n"
                + "     \"type\" : \"string\"\n"
                + "   },\n"
                + "   {\n"
                + "     \"name\" : \"FloatField\",\n"
                + "     \"type\" : \"float\"\n"
                + "   },\n"
                + "   {\n"
                + "     \"name\" : \"BooleanField\",\n"
                + "     \"type\" : \"boolean\"\n"
                + "   }\n"
                + " ]\n"
                + "}";
        Schema schema = AvroUtils.parseSchemaString(avroSchemaString);
        Assert.assertNotNull(schema);
    }
}
