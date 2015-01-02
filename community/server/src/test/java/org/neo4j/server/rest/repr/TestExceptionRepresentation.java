/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.neo4j.server.rest.repr.formats.MapWrappingWriter;

public class TestExceptionRepresentation
{

    @Test
    public void shouldIncludeCause() throws Exception
    {
        // Given
        ExceptionRepresentation rep = new ExceptionRepresentation(
                new RuntimeException("Hoho",
                        new RuntimeException("Haha",
                                new RuntimeException( "HAHA!" )) ));

        Map<String, Object> output = new HashMap<String, Object>();
        MappingSerializer serializer = new MappingSerializer( new MapWrappingWriter(output), URI.create( "" ),
                mock(ExtensionInjector.class ) );

        // When
        rep.serialize( serializer );

        // Then
        assertThat(output.containsKey( "cause" ), is( true ));
        assertThat( output.get( "cause" ), is( instanceOf( Map.class ) ) );
        assertThat( (String) ((Map<String,Object>)output.get( "cause" )).get("message"), is( "Haha" ) );
        assertThat(
                ( (Map<String, Object>) output.get( "cause" ) ).get( "cause" ),
                is( instanceOf( Map.class ) ) );
        assertThat( (String) ((Map<String,Object>)((Map<String,Object>)output.get( "cause" )).get("cause")).get( "message" ),
                is( "HAHA!") );
    }



}
