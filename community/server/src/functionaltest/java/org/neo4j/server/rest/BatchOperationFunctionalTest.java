/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.TestData;

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.UniformInterfaceException;

public class BatchOperationFunctionalTest
{

    public @Rule
    TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );

    private static NeoServerWithEmbeddedWebServer server;
    private static FunctionalTestHelper functionalTestHelper;
    private static GraphDbHelper helper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        server = ServerHelper.createServer();
        functionalTestHelper = new FunctionalTestHelper( server );
        helper = functionalTestHelper.getGraphDbHelper();
    }

    @Before
    public void cleanTheDatabase()
    {
        ServerHelper.cleanTheDatabase( server );
    }

    @AfterClass
    public static void stopServer()
    {
        server.stop();
    }

    /**
     * Execute multiple operations in batch.
     * 
     * This lets you execute multiple API calls through a single HTTP call,
     * significantly improving performance for large insert and update
     * operations.
     * 
     * The batch service expects an array of job descriptions as input, each job
     * description describing an action to be performed via the normal server
     * API.
     * 
     * This service is transactional. If any of the operations performed fails
     * (returns a non-2xx HTTP status code), the transaction will be rolled back
     * and all changes will be undone.
     * 
     * Each job description should contain a +path+ attribute, with a value
     * relative to the data API root (so http://localhost:7474/db/data/node becomes
     * just /node), and a +method+ attribute containing HTTP verb to use.
     * 
     * Optionally you may provide a +body+ attribute, and an +id+ attribute to
     * help you keep track of responses, although responses are guaranteed to be
     * returned in the same order the job descriptions are received.
     * 
     * The following figure outlines the different parts of the job
     * descriptions:
     * 
     * image::batch-request-api.png[]
     */
    @Documented
    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldPerformMultipleOperations() throws Exception {

        String jsonString = new PrettyJSON().array()

                .object()
                .key("method")
                .value("PUT")
                .key("to")
                .value("/node/0/properties")
                .key("body")
                .object()
                .key("age")
                .value(1)
                .endObject()
                .key("id")
                .value(0)
                .endObject()

                .object()
                .key("method")
                .value("GET")
                .key("to")
                .value("/node/0")
                .key("id")
                .value(1)
                .endObject()

                .object()
                .key("method")
                .value("POST")
                .key("to")
                .value("/node")
                .key("body")
                .object()
                .key("age")
                .value(1)
                .endObject()
                .key("id")
                .value(2)
                .endObject()

                .object()
                .key("method")
                .value("POST")
                .key("to")
                .value("/node")
                .key("body")
                .object()
                .key("age")
                .value(1)
                .endObject()
                .key("id")
                .value(3)
                .endObject()

                .endArray()
                .toString();

        String uri = functionalTestHelper.dataUri() + "batch";

        JaxRsResponse response = RestRequest.req().post(uri, jsonString);

        assertEquals(200, response.getStatus());

        List<Map<String, Object>> results = JsonHelper.jsonToList(response.getEntity());

        assertEquals(4, results.size());

        Map<String, Object> putResult = results.get(0);
        Map<String, Object> getResult = results.get(1);
        Map<String, Object> firstPostResult = results.get(2);
        Map<String, Object> secondPostResult = results.get(3);

        // Ids should be ok
        assertEquals(0, putResult.get("id"));
        assertEquals(2, firstPostResult.get("id"));
        assertEquals(3, secondPostResult.get("id"));

        // Should contain "from"
        assertEquals("/node/0/properties", putResult.get("from"));
        assertEquals("/node/0", getResult.get("from"));
        assertEquals("/node", firstPostResult.get("from"));
        assertEquals("/node", secondPostResult.get("from"));

        // Post should contain location
        assertTrue(((String) firstPostResult.get("location")).length() > 0);
        assertTrue(((String) secondPostResult.get("location")).length() > 0);

        // Should have created by the first PUT request
        Map<String, Object> body = (Map<String, Object>) getResult.get("body");
        assertEquals(1, ((Map<String, Object>) body.get("data")).get("age"));

        gen.get()
                .payload(jsonString)
                .expectedStatus(200)
                .post(uri);
    }
    
    /**
     * Refer to items created earlier in the same batch job.
     * 
     * The batch operation API allows you to refer to the URI returned from a
     * created resource in subsequent job descriptions, within the same batch
     * call.
     * 
     * Use the +{[JOB ID]}+ special syntax to inject URIs from created resources
     * into JSON strings in subsequent job descriptions.
     */
    @Documented
    @Test
    public void shouldBeAbleToReferToCreatedResource() throws Exception {
        String jsonString = new PrettyJSON().array()
                .object()
                .key("method")
                .value("POST")
                .key("to")
                .value("/node")
                .key("id")
                .value(0)
                .key("body")
                .object()
                .key("age")
                .value(1)
                .endObject()
                .endObject()
                .object()
                .key("method")
                .value("POST")
                .key("to")
                .value("/node")
                .key("id")
                .value(1)
                .key("body")
                .object()
                .key("age")
                .value(12)
                .endObject()
                .endObject()
                .object()
                .key("method")
                .value("POST")
                .key("to")
                .value("{0}/relationships")
                .key("id")
                .value(3)
                .key("body")
                .object()
                .key("to")
                .value("{1}")
                .key("data")
                .object()
                .key("name")
                .value("bob")
                .endObject()
                .key("type")
                .value("KNOWS")
                .endObject()
                .endObject()
                .object()
                .key("method")
                .value("POST")
                .key("to")
                .value("/index/relationship/my_rels/name/bob")
                .key("id")
                .value(4)
                .key("body")
                .value("{3}")
                .endObject()
                .endArray()
                .toString();

        String uri = functionalTestHelper.dataUri() + "batch";

        JaxRsResponse response = RestRequest.req().post(uri, jsonString);

        assertEquals(200, response.getStatus());

        List<Map<String, Object>> results = JsonHelper.jsonToList(response.getEntity());

        assertEquals(4, results.size());
        assertEquals(1, helper.getIndexedRelationships("my_rels", "name", "bob")
                .size());

        gen.get()
                .payload(jsonString)
                .expectedStatus(200)
                .post(uri);
    }

    @Test
    public void shouldGetLocationHeadersWhenCreatingThings() throws Exception {

        int originalNodeCount = helper.getNumberOfNodes();

        JaxRsResponse response = RestRequest.req().post(functionalTestHelper.dataUri() + "batch", new PrettyJSON().array()

                .object()
                .key("method")
                .value("POST")
                .key("to")
                .value("/node")
                .key("body")
                .object()
                .key("age")
                .value(1)
                .endObject()
                .endObject()

                .endArray()
                .toString());

        assertEquals(200, response.getStatus());
        assertEquals(originalNodeCount + 1, helper.getNumberOfNodes());

        List<Map<String, Object>> results = JsonHelper.jsonToList(response.getEntity());

        assertEquals(1, results.size());

        Map<String, Object> result = results.get(0);
        assertTrue(((String) result.get("location")).length() > 0);
    }

    @Test
    public void shouldForwardUnderlyingErrors() throws Exception {

        JaxRsResponse response = RestRequest.req().post(functionalTestHelper.dataUri() + "batch", new PrettyJSON()
            .array()
                .object()
                    .key("method") .value("POST")
                    .key("to")     .value("/node")
                    .key("body")   
                        .object()
                            .key("age")  .array().value(true).value("hello").endArray()
                        .endObject()
                .endObject()
            .endArray()
            .toString());
        assertEquals(400, response.getStatus());
        Map<String, Object> res = JsonHelper.jsonToMap(response.getEntity());

        assertTrue(((String)res.get("message")).startsWith("Invalid JSON array in POST body"));
    }
    
    @Test
    public void shouldRollbackAllWhenGivenIncorrectRequest() throws JsonParseException, ClientHandlerException,
            UniformInterfaceException {

        String jsonString = "[" + "{ " + "\"method\":\"POST\"," + "\"to\":\"/node\", " + "\"body\":{ \"age\":1 }"
                + "}," + "{ " + "\"method\":\"POST\"," + "\"to\":\"/node\", "
                + "\"body\":[\"a_list\",\"this_makes_no_sense\"]" + "}" + "]";

        int originalNodeCount = helper.getNumberOfNodes();

        JaxRsResponse response = RestRequest.req().post(functionalTestHelper.dataUri() + "batch", jsonString);

        assertEquals(400, response.getStatus());
        assertEquals(originalNodeCount, helper.getNumberOfNodes());

    }

    @Test
    public void shouldRollbackAllWhenInsertingIllegalData() throws JsonParseException, ClientHandlerException,
            UniformInterfaceException {

        String jsonString = "[" + "{ " + "\"method\":\"POST\"," + "\"to\":\"/node\", " + "\"body\":{ \"age\":1 }"
                + "}," + "{ " + "\"method\":\"POST\"," + "\"to\":\"/node\", "
                + "\"body\":{ \"age\":{ \"age\":{ \"age\":1 } } }" + "}" + "]";

        int originalNodeCount = helper.getNumberOfNodes();

        JaxRsResponse response = RestRequest.req().post(functionalTestHelper.dataUri() + "batch", jsonString);

        assertEquals(400, response.getStatus());
        assertEquals(originalNodeCount, helper.getNumberOfNodes());

    }

    @Test
    public void shouldRollbackAllOnSingle404() throws JsonParseException, ClientHandlerException,
            UniformInterfaceException {

        String jsonString = "[" + "{ " + "\"method\":\"POST\"," + "\"to\":\"/node\", " + "\"body\":{ \"age\":1 }"
                + "}," + "{ " + "\"method\":\"POST\"," + "\"to\":\"www.google.com\"" + "}" + "]";

        int originalNodeCount = helper.getNumberOfNodes();

        JaxRsResponse response = RestRequest.req().post(functionalTestHelper.dataUri() + "batch", jsonString);

        assertEquals(400, response.getStatus());
        assertEquals(originalNodeCount, helper.getNumberOfNodes());

    }
    
    @Ignore
    @Test
    public void testlargerequest() {    
        testlargerequest(1000);
        testlargerequest(5000);
        testlargerequest(10000);
        testlargerequest(50000);
        testlargerequest(100000);
        testlargerequest(500000);
        testlargerequest(1000000);
    }
    
    private void testlargerequest(int number) {
        StringBuilder largereq = new StringBuilder();
        largereq.append("[");
        for(int i=0;i<number; i++) {
            largereq.append("{\"method\":\"post\",\"to\":\"/node\", \"body\":{ \"age\":1 } },");
        }
        largereq.append("{\"method\":\"post\",\"to\":\"/node\", \"body\":{ \"age\":1 } }]");
        
        System.out.println(Runtime.getRuntime().totalMemory());
        
        Date d = new Date();
        JaxRsResponse response = RestRequest.req().post(functionalTestHelper.dataUri() + "batch", largereq.toString());

        System.out.println(number + "\t:" + ((new Date()).getTime() - d.getTime()) + "ms");
        
        assertEquals(200, response.getStatus());
        
    }
}
