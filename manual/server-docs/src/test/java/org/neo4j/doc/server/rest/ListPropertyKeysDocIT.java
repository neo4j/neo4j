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

import java.util.List;
import java.util.Set;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription;

import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.server.rest.domain.JsonHelper.readJson;

public class ListPropertyKeysDocIT extends org.neo4j.doc.server.rest.AbstractRestFunctionalTestBase
{
    @Test
    @Documented( "List all property keys." )
    @GraphDescription.Graph( nodes = {
            @GraphDescription.NODE( name = "a", setNameProperty = true ),
            @GraphDescription.NODE( name = "b", setNameProperty = true ),
            @GraphDescription.NODE( name = "c", setNameProperty = true )
    } )
    public void list_all_property_keys_ever_used() throws JsonParseException
    {
        data.get();
        String uri = getPropertyKeysUri();
        String body = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .get( uri )
                .entity();

        Set<?> parsed = asSet((List<?>) readJson( body ));
        assertTrue( parsed.contains( "name" ) );
    }
}
