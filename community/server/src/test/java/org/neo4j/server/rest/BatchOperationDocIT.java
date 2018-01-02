/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.UniformInterfaceException;
import org.codehaus.jackson.JsonNode;
import org.json.JSONException;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Neo4jMatchers.hasProperty;
import static org.neo4j.graphdb.Neo4jMatchers.inTx;

public class BatchOperationDocIT extends AbstractRestFunctionalDocTestBase
{

    @Documented( "Execute multiple operations in batch.\n" +
                 "\n" +
                 "The batch service expects an array of job descriptions as input, each job\n" +
                 "description describing an action to be performed via the normal server\n" +
                 "API.\n" +
                 "\n" +
                 "Each job description should contain a +to+ attribute, with a value\n" +
                 "relative to the data API root (so http://localhost:7474/db/data/node becomes\n" +
                 "just /node), and a +method+ attribute containing HTTP verb to use.\n" +
                 "\n" +
                 "Optionally you may provide a +body+ attribute, and an +id+ attribute to\n" +
                 "help you keep track of responses, although responses are guaranteed to be\n" +
                 "returned in the same order the job descriptions are received.\n" +
                 "\n" +
                 "The following figure outlines the different parts of the job\n" +
                 "descriptions:\n" +
                 "\n" +
                 "image::batch-request-api.png[]" )
    @SuppressWarnings( "unchecked" )
    @Test
    @Graph("Joe knows John")
    public void shouldPerformMultipleOperations() throws Exception
    {
        long idJoe = data.get().get( "Joe" ).getId();
        String jsonString = new PrettyJSON()
            .array()
                .object()
                    .key("method")  .value("PUT")
                    .key("to")      .value("/node/" + idJoe + "/properties")
                    .key("body")
                        .object()
                            .key("age").value(1)
                        .endObject()
                    .key("id")      .value(0)
                .endObject()
                .object()
                    .key("method")  .value("GET")
                    .key("to")      .value("/node/" + idJoe)
                    .key("id")      .value(1)
                .endObject()
                .object()
                    .key("method")  .value("POST")
                    .key("to")      .value("/node")
                    .key("body")
                        .object()
                            .key("age").value(1)
                        .endObject()
                    .key("id")      .value(2)
                .endObject()
                .object()
                    .key("method")  .value("POST")
                    .key("to")      .value("/node")
                    .key("body")
                        .object()
                            .key("age").value(1)
                        .endObject()
                    .key("id")      .value(3)
                .endObject()
            .endArray().toString();


        String entity = gen.get()
        .description( startGraph( "execute multiple operations in batch" ) )
        .payload(jsonString)
        .expectedStatus(200)
        .post(batchUri()).entity();

        List<Map<String, Object>> results = JsonHelper.jsonToList(entity);

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
        assertEquals("/node/"+idJoe+"/properties", putResult.get("from"));
        assertEquals("/node/"+idJoe, getResult.get("from"));
        assertEquals("/node", firstPostResult.get("from"));
        assertEquals("/node", secondPostResult.get("from"));

        // Post should contain location
        assertTrue(((String) firstPostResult.get("location")).length() > 0);
        assertTrue(((String) secondPostResult.get("location")).length() > 0);

        // Should have created by the first PUT request
        Map<String, Object> body = (Map<String, Object>) getResult.get("body");
        assertEquals(1, ((Map<String, Object>) body.get("data")).get("age"));


    }

    @Documented( "Refer to items created earlier in the same batch job.\n" +
                 "\n" +
                 "The batch operation API allows you to refer to the URI returned from a\n" +
                 "created resource in subsequent job descriptions, within the same batch\n" +
                 "call.\n" +
                 "\n" +
                 "Use the +{[JOB ID]}+ special syntax to inject URIs from created resources\n" +
                 "into JSON strings in subsequent job descriptions." )
    @Test
    public void shouldBeAbleToReferToCreatedResource() throws Exception
    {
        String jsonString = new PrettyJSON()
            .array()
                .object()
                    .key("method")  .value("POST")
                    .key("to")      .value("/node")
                    .key("id")      .value(0)
                    .key("body")
                        .object()
                            .key("name").value("bob")
                        .endObject()
                .endObject()
                .object()
                    .key("method")  .value("POST")
                    .key("to")      .value("/node")
                    .key("id")      .value(1)
                    .key("body")
                        .object()
                            .key("age").value(12)
                        .endObject()
                .endObject()
                .object()
                    .key("method")  .value("POST")
                    .key("to")      .value("{0}/relationships")
                    .key("id")      .value(3)
                    .key("body")
                        .object()
                            .key("to").value("{1}")
                            .key("data")
                                .object()
                                    .key("since").value("2010")
                                .endObject()
                            .key("type").value("KNOWS")
                        .endObject()
                .endObject()
                .object()
                    .key("method")  .value("POST")
                    .key("to")      .value("/index/relationship/my_rels")
                    .key("id")      .value(4)
                    .key("body")
                        .object()
                            .key("key").value("since")
                            .key("value").value("2010")
                            .key("uri").value("{3}")
                        .endObject()
                .endObject()
            .endArray().toString();

        String entity = gen.get()
        .expectedStatus( 200 )
        .payload( jsonString )
        .post( batchUri() )
        .entity();

        List<Map<String, Object>> results = JsonHelper.jsonToList(entity);

        assertEquals(4, results.size());

//        String rels = gen.get()
//                .expectedStatus( 200 ).get( getRelationshipIndexUri( "my_rels", "since", "2010")).entity();
//        assertEquals(1, JsonHelper.jsonToList(  rels ).size());
    }

    private String batchUri()
    {
        return getDataUri()+"batch";
    }

    @Test
    public void shouldGetLocationHeadersWhenCreatingThings() throws Exception
    {
        int originalNodeCount = countNodes();

        final String jsonString = new PrettyJSON()
            .array()
                .object()
                    .key("method").value("POST")
                    .key("to").value("/node")
                    .key("body")
                        .object()
                            .key("age").value(1)
                        .endObject()
                .endObject()
            .endArray().toString();

        JaxRsResponse response = RestRequest.req().post(batchUri(), jsonString);

        assertEquals(200, response.getStatus());
        assertEquals(originalNodeCount + 1, countNodes());

        List<Map<String, Object>> results = JsonHelper.jsonToList(response.getEntity());

        assertEquals(1, results.size());

        Map<String, Object> result = results.get(0);
        assertTrue(((String) result.get("location")).length() > 0);
    }

    @Test
    public void shouldForwardUnderlyingErrors() throws Exception
    {
        JaxRsResponse response = RestRequest.req().post(batchUri(), new PrettyJSON()
            .array()
                .object()
                    .key("method") .value("POST")
                    .key("to")     .value("/node")
                    .key("body")
                        .object()
                            .key("age")
                                .array()
                                    .value(true)
                                    .value("hello")
                                .endArray()
                        .endObject()
                .endObject()
            .endArray()
            .toString());
        assertEquals(500, response.getStatus());
        Map<String, Object> res = JsonHelper.jsonToMap(response.getEntity());

        assertTrue(((String)res.get("message")).startsWith("Invalid JSON array in POST body"));
    }

    @Test
    public void shouldRollbackAllWhenGivenIncorrectRequest() throws ClientHandlerException,
            UniformInterfaceException, JSONException
    {

        String jsonString = new PrettyJSON()
            .array()
                .object()
                    .key("method") .value("POST")
                    .key("to")     .value("/node")
                    .key("body")
                        .object()
                            .key("age").value("1")
                        .endObject()
                .endObject()
                .object()
                    .key("method") .value("POST")
                    .key("to")     .value("/node")
                    .key("body")
                        .array()
                            .value("a_list")
                            .value("this_makes_no_sense")
                        .endArray()
                .endObject()
            .endArray()
            .toString();

        int originalNodeCount = countNodes();

        JaxRsResponse response = RestRequest.req().post(batchUri(), jsonString);

        assertEquals(500, response.getStatus());
        assertEquals(originalNodeCount, countNodes());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldHandleUnicodeGetCorrectly() throws Exception
    {
        String asianText = "\u4f8b\u5b50";
        String germanText = "öäüÖÄÜß";

        String complicatedString = asianText + germanText;

        String jsonString = new PrettyJSON()
            .array()
                .object()
                    .key("method") .value("POST")
                    .key("to")     .value("/node")
                    .key("body")   .object()
                                       .key(complicatedString).value(complicatedString)
                                   .endObject()
                .endObject()
            .endArray()
            .toString();

        String entity = gen.get()
                .expectedStatus( 200 )
                .payload( jsonString )
                .post( batchUri() )
                .entity();

        // Pull out the property value from the depths of the response
        Map<String, Object> response = (Map<String, Object>) JsonHelper.jsonToList(entity).get(0).get("body");
        String returnedValue = (String)((Map<String,Object>)response.get("data")).get(complicatedString);

        // Ensure nothing was borked.
        assertThat("Expected twisted unicode case to work, but response was: " + entity,
                returnedValue, is(complicatedString));
    }

    @Test
    public void shouldHandleFailingCypherStatementCorrectly() throws Exception
    {
        String jsonString = new PrettyJSON()
            .array()
                .object()
                    .key("method") .value("POST")
                    .key("to")     .value("/cypher")
                    .key("body")   .object()
                                       .key("query").value("create (n) set n.foo = {maps:'not welcome'} return n")
                                       .key("params").object().key("id").value("0").endObject()
                                   .endObject()
                .endObject()
                .object()
                    .key("method") .value("POST")
                    .key("to")     .value("/node")
                .endObject()
            .endArray()
            .toString();

        String entity = gen.get()
                .expectedStatus( 500 )
                .payload( jsonString )
                .post( batchUri() )
                .entity();

        // Pull out the property value from the depths of the response
        Map<String, Object> result = JsonHelper.jsonToMap(entity);
        String exception = (String) result.get("exception");
        assertThat(exception, is("BatchOperationFailedException"));
        String innerException = (String) ((Map) JsonHelper.jsonToMap((String) result.get("message"))).get("exception");
        assertThat(innerException, is("CypherTypeException"));
    }

    @Test
    @Graph("Peter likes Jazz")
    public void shouldHandleEscapedStrings() throws ClientHandlerException,
            UniformInterfaceException, JSONException, JsonParseException
    {
    	String string = "Jazz";
        Node gnode = getNode( string );
        assertThat( gnode, inTx(graphdb(), hasProperty( "name" ).withValue(string)) );

        String name = "string\\ and \"test\"";

        String jsonString = new PrettyJSON()
        .array()
            .object()
                .key("method") .value("PUT")
                .key("to")     .value("/node/"+gnode.getId()+"/properties")
                .key("body")
                    .object()
                        .key("name").value(name)
                    .endObject()
            .endObject()
        .endArray()
        .toString();
        gen.get()
            .expectedStatus( 200 )
            .payload( jsonString )
            .post( batchUri() )
            .entity();

        jsonString = new PrettyJSON()
        .array()
            .object()
                .key("method") .value("GET")
                .key("to")     .value("/node/"+gnode.getId()+"/properties/name")
            .endObject()
        .endArray()
        .toString();
        String entity = gen.get()
            .expectedStatus( 200 )
            .payload( jsonString )
            .post( batchUri() )
            .entity();

        List<Map<String, Object>> results = JsonHelper.jsonToList(entity);
        assertEquals(results.get(0).get("body"), name);
    }

    @Test
    public void shouldRollbackAllWhenInsertingIllegalData() throws ClientHandlerException,
            UniformInterfaceException, JSONException
    {

        String jsonString = new PrettyJSON()
            .array()
                .object()
                    .key("method")  .value("POST")
                    .key("to")      .value("/node")
                    .key("body")
                        .object()
                            .key("age").value(1)
                        .endObject()
                .endObject()

                .object()
                    .key("method").value("POST")
                    .key("to").value("/node")
                    .key("body")
                        .object()
                            .key("age")
                                .object()
                                    .key("age").value(1)
                                .endObject()
                        .endObject()
                .endObject()

            .endArray().toString();

        int originalNodeCount = countNodes();

        JaxRsResponse response = RestRequest.req().post(batchUri(), jsonString);

        assertEquals(500, response.getStatus());
        assertEquals(originalNodeCount, countNodes());

    }

    @Test
    public void shouldRollbackAllOnSingle404() throws ClientHandlerException,
            UniformInterfaceException, JSONException
    {

        String jsonString = new PrettyJSON()
            .array()
                .object()
                    .key("method")  .value("POST")
                    .key("to")      .value("/node")
                    .key("body")
                        .object()
                            .key("age").value(1)
                        .endObject()
                .endObject()
                .object()
                    .key("method")  .value("POST")
                    .key("to")      .value("www.google.com")
                .endObject()

            .endArray().toString();

        int originalNodeCount = countNodes();

        JaxRsResponse response = RestRequest.req().post(batchUri(), jsonString);

        assertEquals(500, response.getStatus());
        assertEquals(originalNodeCount, countNodes());
    }

    @Test
    public void shouldBeAbleToReferToUniquelyCreatedEntities() throws Exception
    {
        String jsonString = new PrettyJSON()
            .array()
                .object()
                    .key("method")  .value("POST")
                    .key("to")      .value("/index/node/Cultures?unique")
                    .key("body")
                        .object()
                            .key("key").value("ID")
                            .key("value").value("fra")
                            .key("properties")
                                .object()
                                    .key("ID").value("fra")
                                .endObject()
                        .endObject()
                    .key("id")      .value(0)
                .endObject()
                .object()
                    .key("method")  .value("POST")
                    .key("to")      .value("/node")
                    .key("id")      .value(1)
                .endObject()
                .object()
                    .key("method")  .value("POST")
                    .key("to")      .value("{1}/relationships")
                    .key("body")
                        .object()
                            .key("to").value("{0}")
                            .key("type").value("has")
                        .endObject()
                    .key("id")      .value(2)
                .endObject()
            .endArray().toString();

        JaxRsResponse response = RestRequest.req().post(batchUri(), jsonString);

        assertEquals(200, response.getStatus());

    }

    @Test
    public void shouldNotFailWhenRemovingAndAddingLabelsInOneBatch() throws Exception
    {
        // given

        /*
        curl -X POST http://localhost:7474/db/data/batch -H 'Content-Type: application/json'
        -d '[
           {"body":{"name":"Alice"},"to":"node","id":0,"method":"POST"},
           {"body":["expert","coder"],"to":"{0}/labels","id":1,"method":"POST"},
           {"body":["novice","chef"],"to":"{0}/labels","id":2,"method":"PUT"}
        ]'
        */

        String jsonString = new PrettyJSON()
            .array()
                .object()
                    .key("method")  .value("POST")
                    .key("to")      .value("node")
                    .key("id")      .value(0)
                    .key("body")
                    .object()
                        .key("key").value("name")
                        .key("value").value("Alice")
                    .endObject()
                .endObject()
                .object()
                    .key("method")  .value("POST")
                    .key("to")      .value("{0}/labels")
                    .key("id")      .value(1)
                    .key("body")
                    .array()
                        .value( "expert" )
                        .value( "coder" )
                    .endArray()
                .endObject()
                .object()
                    .key("method")  .value("PUT")
                    .key("to")      .value("{0}/labels")
                    .key("id")      .value(2)
                    .key("body")
                    .array()
                        .value( "novice" )
                        .value( "chef" )
                    .endArray()
                .endObject()
             .endArray().toString();

        // when
        JaxRsResponse response = RestRequest.req().post(batchUri(), jsonString);

        // then
        assertEquals(200, response.getStatus());
    }

    // It has to be possible to create relationships among created and not-created nodes
    // in batch operation.  Tests the fix for issue #690.
    @Test
    public void shouldBeAbleToReferToNotCreatedUniqueEntities() throws Exception
    {
        String jsonString = new PrettyJSON()
            .array()
                .object()
                    .key("method")  .value("POST")
                    .key("to")      .value("/index/node/Cultures?unique")
                    .key("body")
                        .object()
                            .key("key").value("name")
                            .key("value").value("tobias")
                            .key("properties")
                                .object()
                                    .key("name").value("Tobias Tester")
                                .endObject()
                        .endObject()
                    .key("id")      .value(0)
                .endObject()
                .object()                       // Creates Andres, hence 201 Create
                    .key("method")  .value("POST")
                    .key("to")      .value("/index/node/Cultures?unique")
                    .key("body")
                        .object()
                            .key("key").value("name")
                            .key("value").value("andres")
                            .key("properties")
                                .object()
                                    .key("name").value("Andres Tester")
                                .endObject()
                        .endObject()
                    .key("id")      .value(1)
                .endObject()
                .object()                       // Duplicated to ID.1, hence 200 OK
                    .key("method")  .value("POST")
                    .key("to")      .value("/index/node/Cultures?unique")
                    .key("body")
                        .object()
                            .key("key").value("name")
                            .key("value").value("andres")
                            .key("properties")
                                .object()
                                    .key("name").value("Andres Tester")
                                .endObject()
                        .endObject()
                    .key("id")      .value(2)
                .endObject()
                .object()
                    .key("method")  .value("POST")
                    .key("to")      .value("/index/relationship/my_rels/?unique")
                    .key("body")
                        .object()
                            .key("key").value("name")
                            .key("value").value("tobias-andres")
                            .key("start").value("{0}")
                            .key("end").value("{1}")
                            .key("type").value("FRIENDS")
                        .endObject()
                    .key("id")      .value(3)
                .endObject()
                .object()
                    .key("method")  .value("POST")
                    .key("to")      .value("/index/relationship/my_rels/?unique")
                    .key("body")
                        .object()
                            .key("key").value("name")
                            .key("value").value("andres-tobias")
                            .key("start").value("{2}")          // Not-created entity here
                            .key("end").value("{0}")
                            .key("type").value("FRIENDS")
                        .endObject()
                    .key("id")      .value(4)
                .endObject()
                .object()
                    .key("method")  .value("POST")
                    .key("to")      .value("/index/relationship/my_rels/?unique")
                    .key("body")
                        .object()
                            .key("key").value("name")
                            .key("value").value("andres-tobias")
                            .key("start").value("{1}")          // Relationship should not be created
                            .key("end").value("{0}")
                            .key("type").value("FRIENDS")
                        .endObject()
                    .key("id")      .value(5)
                .endObject()
            .endArray().toString();

        JaxRsResponse response = RestRequest.req().post(batchUri(), jsonString);

        assertEquals(200, response.getStatus());

        final String entity = response.getEntity();
        List<Map<String, Object>> results = JsonHelper.jsonToList(entity);
        assertEquals(6, results.size());
        Map<String, Object> andresResult1 = results.get(1);
        Map<String, Object> andresResult2 = results.get(2);
        Map<String, Object> secondRelationship  = results.get(4);
        Map<String, Object> thirdRelationship  = results.get(5);

        // Same people
        Map<String, Object> body1 = (Map<String, Object>) andresResult1.get("body");
        Map<String, Object> body2 = (Map<String, Object>) andresResult2.get("body");
        assertEquals(body1.get("id"), body2.get("id"));
        // Same relationship
        body1 = (Map<String, Object>) secondRelationship.get("body");
        body2 = (Map<String, Object>) thirdRelationship.get("body");
        assertEquals(body1.get("self"), body2.get("self"));
        // Created for {2} {0}
        assertTrue(((String) secondRelationship.get("location")).length() > 0);
        // {2} = {1} = Andres
        body1 = (Map<String, Object>) secondRelationship.get("body");
        body2 = (Map<String, Object>) andresResult1.get("body");
        assertEquals(body1.get("start"), body2.get("self"));
    }

    @Test
    public void shouldFailWhenUsingPeriodicCommitViaNewTxEndpoint() throws Exception
    {
        ServerTestUtils.withCSVFile( 1, new ServerTestUtils.BlockWithCSVFileURL()
        {
            @Override
            public void execute( String url ) throws Exception
            {
                // Given
                String jsonString = new PrettyJSON()
                        .array()
                        .object()
                        .key( "method" ).value("POST")
                        .key( "to" ).value("/transaction/commit")
                        .key( "body" ).object()
                        .key("statements").array()
                        .object().key( "statement" ).value( "USING PERIODIC COMMIT LOAD CSV FROM '" + url + "' AS line CREATE ()" ).endObject()
                        .endArray()
                        .endObject()
                        .endObject()
                        .endArray()
                        .toString();

                // When
                JsonNode result = JsonHelper.jsonNode(gen.get()
                        .expectedStatus(200)
                        .payload(jsonString)
                        .post(batchUri())
                        .entity());

                // Then
                JsonNode results = result.get(0).get("body").get("results");
                JsonNode errors = result.get(0).get("body").get("errors");

                assertTrue( "Results not an array", results.isArray() );
                assertTrue( "Results not empty", 0 == results.size() );
                assertTrue( "Errors not an array", errors.isArray() );
                assertTrue("Didn't find exactly one error", 1 == errors.size());

                String errorCode = errors.get(0).get("code").getTextValue();
                assertEquals( "Neo.ClientError.Statement.InvalidSemantics", errorCode );
            }
        } );
    }

    private int countNodes()
    {
        try ( Transaction tx = graphdb().beginTx() )
        {
            int count = 0;
            for(Node node : GlobalGraphOperations.at(graphdb()).getAllNodes())
            {
                count++;
            }
            return count;
        }
    }
}
