/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.doc.server.rest.repr;

import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RepresentationTestBase
{
    static final String NODE_URI_PATTERN = "http://.*/node/[0-9]+";
    static final String RELATIONSHIP_URI_PATTERN = "http://.*/relationship/[0-9]+";

    static void assertUriMatches( String expectedRegex, String actualUri )
    {
        assertTrue( "expected <" + expectedRegex + "> got <" + actualUri + ">", actualUri.matches( expectedRegex ) );
    }

    private RepresentationTestBase()
    {
        // only static resource
    }

    public static void verifySerialisation( Map<String, Object> relrep )
    {
        assertUriMatches( RELATIONSHIP_URI_PATTERN, relrep.get( "self" )
                .toString() );
        assertUriMatches( NODE_URI_PATTERN, relrep.get( "start" )
                .toString() );
        assertUriMatches( NODE_URI_PATTERN, relrep.get( "end" )
                .toString() );
        assertNotNull( relrep.get( "type" ) );
        assertUriMatches( RELATIONSHIP_URI_PATTERN + "/properties", relrep.get( "properties" )
                .toString() );
        assertUriMatches( RELATIONSHIP_URI_PATTERN + "/properties/\\{key\\}", (String) relrep.get( "property" ) );
        assertNotNull( relrep.get( "data" ) );
        assertNotNull( relrep.get( "metadata" ) );
        Map metadata = (Map) relrep.get( "metadata" );
        assertNotNull( metadata.get("type") );
        assertTrue( ( (Number) metadata.get("id") ).longValue() >= 0 );
    }
}
