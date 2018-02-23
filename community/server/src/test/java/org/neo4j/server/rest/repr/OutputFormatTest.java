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

import org.junit.jupiter.api.Test;

import java.net.URI;
import javax.ws.rs.core.Response;

import org.neo4j.server.rest.repr.formats.JsonFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OutputFormatTest
{
    @Test
    public void shouldReturnAbsoluteURIForSeeOther() throws Exception
    {
        URI relativeURI = new URI( "/test/path" );

        OutputFormat outputFormat = new OutputFormat( new JsonFormat(), new URI( "http://base.local:8765/" ), null );

        Response response = outputFormat.seeOther( relativeURI );

        assertEquals( 303, response.getStatus() );
        assertEquals( new URI( "http://base.local:8765/test/path" ), response.getMetadata().getFirst( "Location" ) );
    }
}
