/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.doc.server.rest;

import org.junit.Test;

import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription;

import static junit.framework.TestCase.assertEquals;

public class DegreeDocIT extends org.neo4j.doc.server.rest.AbstractRestFunctionalTestBase
{
    @Documented( "Get the degree of a node\n" +
                 "\n" +
                 "Return the total number of relationships associated with a node." )
    @Test
    @GraphDescription.Graph( {"Root knows Mattias", "Root knows Johan"} )
    public void get_degree() throws JsonParseException
    {
        Map<String,Node> nodes = data.get();
        String nodeUri = getNodeUri( nodes.get( "Root" ) );

        // Document
        org.neo4j.doc.server.rest.RESTDocsGenerator.ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .get( nodeUri + "/degree/all" );

        // Then
        assertEquals( 2, JsonHelper.jsonNode( response.response().getEntity() ).asInt() );
    }

    @Documented( "Get the degree of a node by direction\n" +
                 "\n" +
                 "Return the number of relationships of a particular direction for a node.\n" +
                 "Specify `all`, `in` or `out`." )
    @Test
    @GraphDescription.Graph( {"Root knows Mattias", "Root knows Johan"} )
    public void get_degree_by_direction() throws JsonParseException
    {
        Map<String,Node> nodes = data.get();
        String nodeUri = getNodeUri( nodes.get( "Root" ) );

        // Document
        org.neo4j.doc.server.rest.RESTDocsGenerator.ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .get( nodeUri + "/degree/out" );

        // Then
        assertEquals( 2, JsonHelper.jsonNode( response.response().getEntity() ).asInt() );
    }

    @Documented( "Get the degree of a node by direction and types\n" +
                 "\n" +
                 "If you are only interested in the degree of a particular relationship type, or a set of relationship types, you specify relationship types after the direction.\n" +
                 "You can combine multiple relationship types by using the `&` character." )
    @Test
    @GraphDescription.Graph( {"Root KNOWS Mattias", "Root KNOWS Johan", "Root LIKES Cookie"} )
    public void get_degree_by_direction_and_type() throws JsonParseException
    {
        Map<String,Node> nodes = data.get();
        String nodeUri = getNodeUri( nodes.get( "Root" ) );

        // Document
        org.neo4j.doc.server.rest.RESTDocsGenerator.ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .get( nodeUri + "/degree/out/KNOWS&LIKES" );

        // Then
        assertEquals( 3, JsonHelper.jsonNode( response.response().getEntity() ).asInt() );
    }
}
