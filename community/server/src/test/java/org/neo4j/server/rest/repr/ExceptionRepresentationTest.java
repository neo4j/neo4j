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
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.repr.formats.MapWrappingWriter;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.neo4j.kernel.api.exceptions.Status.General.UnknownFailure;

public class ExceptionRepresentationTest
{

    @Test
    public void shouldIncludeCause() throws Exception
    {
        // Given
        ExceptionRepresentation rep = new ExceptionRepresentation(
                new RuntimeException("Hoho", new RuntimeException("Haha", new RuntimeException( "HAHA!" )) ));

        // When
        JsonNode out = serialize( rep );

        // Then
        assertThat( out.get("cause").get("message").asText(), is( "Haha" ) );
        assertThat( out.get( "cause" ).get("cause").get("message").asText(), is( "HAHA!") );
    }

    @Test
    public void shouldRenderErrorsWithNeo4jStatusCode() throws Exception
    {
        // Given
        ExceptionRepresentation rep = new ExceptionRepresentation( new KernelException( UnknownFailure, "Hello" ) { });

        // When
        JsonNode out = serialize( rep );

        // Then
        assertThat(out.get("errors").get(0).get("code").asText(), equalTo("Neo.DatabaseError.General.UnknownFailure"));
        assertThat(out.get("errors").get(0).get("message").asText(), equalTo("Hello"));
    }

    @Test
    public void shoudExcludeLegacyFormatIfAsked() throws Exception
    {
        // Given
        ExceptionRepresentation rep = new ExceptionRepresentation( new KernelException( UnknownFailure, "Hello" ) { }, /*legacy*/false);

        // When
        JsonNode out = serialize( rep );

        // Then
        assertThat(out.get("errors").get(0).get("code").asText(), equalTo("Neo.DatabaseError.General.UnknownFailure"));
        assertThat(out.get("errors").get(0).get("message").asText(), equalTo("Hello"));
        assertThat(out.has( "message" ), equalTo(false));
    }

    private JsonNode serialize( ExceptionRepresentation rep ) throws JsonParseException
    {
        Map<String, Object> output = new HashMap<>();
        MappingSerializer serializer = new MappingSerializer( new MapWrappingWriter(output), URI.create( "" ),
                mock(ExtensionInjector.class ) );

        // When
        rep.serialize( serializer );
        return JsonHelper.jsonNode( JsonHelper.createJsonFrom( output ) );
    }
}
