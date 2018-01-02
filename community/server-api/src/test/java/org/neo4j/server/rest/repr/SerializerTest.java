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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.net.URI;

import org.junit.Test;


public class SerializerTest
{
    
    @Test 
    public void shouldPrependBaseUriToRelativePaths() {
        String baseUrl = "http://baseurl/";
        Serializer serializer = new Serializer(URI.create( baseUrl ), null){};
        
        String aRelativeUrl = "/path/path/path";
        assertThat(serializer.relativeUri( aRelativeUrl ), is( baseUrl + aRelativeUrl.substring( 1 ) ));
        assertThat(serializer.relativeTemplate( aRelativeUrl ), is( baseUrl + aRelativeUrl.substring( 1 ) ));
    }
    
}
