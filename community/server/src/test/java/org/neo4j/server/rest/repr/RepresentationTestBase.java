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
package org.neo4j.server.rest.repr;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RepresentationTestBase
{
    static final URI BASE_URI = URI.create( "http://neo4j.org/" );
    static final String NODE_URI_PATTERN = "http://.*/node/[0-9]+";
    static final String RELATIONSHIP_URI_PATTERN = "http://.*/relationship/[0-9]+";

    static void assertUriMatches( String expectedRegex, ValueRepresentation uriRepr )
    {
        assertUriMatches( expectedRegex, RepresentationTestAccess.serialize( uriRepr ) );
    }

    static void assertUriMatches( String expectedRegex, String actualUri )
    {
        assertTrue( actualUri.matches( expectedRegex ), "expected <" + expectedRegex + "> got <" + actualUri + ">" );
    }

    static String uriPattern( String subPath )
    {
        return "http://.*/[0-9]+" + subPath;
    }

    private RepresentationTestBase()
    {
        // only static resource
    }
}
