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

import org.junit.Test;

import java.util.List;
import java.util.Set;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription;

import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.server.rest.domain.JsonHelper.readJson;

public class ListPropertyKeysDocIT extends AbstractRestFunctionalTestBase
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
