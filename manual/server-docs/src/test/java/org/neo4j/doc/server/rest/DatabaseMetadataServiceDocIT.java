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

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.allOf;
import static org.junit.Assert.assertThat;

public class DatabaseMetadataServiceDocIT extends org.neo4j.doc.server.rest.AbstractRestFunctionalTestBase
{
    private static FunctionalTestHelper functionalTestHelper;
    private static GraphDbHelper helper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
        helper = functionalTestHelper.getGraphDbHelper();
    }

    @Documented( "Get relationship types." )
    @Test
    public void shouldReturn200OnGet()
    {
        helper.createRelationship( "KNOWS" );
        helper.createRelationship( "LOVES" );

        String result = gen.get()
                .noGraph() // add back the graph when we have a clean graph really
                .expectedStatus( 200 )
                .get( functionalTestHelper.dataUri() + "relationship/types" )
                .entity();
        assertThat( result, allOf( containsString( "KNOWS" ), containsString( "LOVES" ) ) );
    }
}
