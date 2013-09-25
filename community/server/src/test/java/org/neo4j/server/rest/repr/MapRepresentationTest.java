/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.rest.repr;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;
import org.neo4j.server.rest.repr.formats.JsonFormat;

import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.map;

public class MapRepresentationTest
{
    @Test
    public void shouldHaveKeys() throws BadInputException, URISyntaxException
    {
        MapRepresentation rep = new MapRepresentation( map( "name", "John", "age", 23, "hobby", null ) );
        OutputFormat format = new OutputFormat( new JsonFormat(), new URI( "http://localhost/" ), null );
        String assemble = format.assemble( rep );
        assertTrue( assemble.contains( "\"age\" : 23" ) );
        assertTrue( assemble.contains( "\"hobby\" : null" ) );
    }
}
