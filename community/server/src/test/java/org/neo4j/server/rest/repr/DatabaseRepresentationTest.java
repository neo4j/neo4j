/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.Test;

import java.net.URI;
import java.util.HashMap;

import org.neo4j.server.rest.repr.formats.MapWrappingWriter;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.mockito.Mockito.mock;

public class DatabaseRepresentationTest
{
    @Test
    public void shouldIncludeExpectedResourcePaths()
    {
        // Given
        DatabaseRepresentation repr = new DatabaseRepresentation();

        // When
        HashMap<String,Object> output = new HashMap<>();
        repr.serialize( new MappingSerializer( new MapWrappingWriter( output ), URI.create("http://steveformayor.org"), mock(ExtensionInjector.class) ) );

        // Then
        assertThat( output, hasEntry( "relationship_index", "http://steveformayor.org/index/relationship" ) );
        assertThat( output, hasEntry( "relationship_index", "http://steveformayor.org/index/relationship" ) );
        assertThat( output, hasEntry( "node_index", "http://steveformayor.org/index/node" ) );
        assertThat( output, hasEntry( "batch", "http://steveformayor.org/batch" ) );
        assertThat( output, hasEntry( "constraints", "http://steveformayor.org/schema/constraint" ) );
        assertThat( output, hasEntry( "node", "http://steveformayor.org/node" ) );
        assertThat( output, hasEntry( "extensions_info", "http://steveformayor.org/ext" ) );
        assertThat( output, hasEntry( "node_labels", "http://steveformayor.org/labels" ) );
        assertThat( output, hasEntry( "indexes", "http://steveformayor.org/schema/index" ) );
        assertThat( output, hasEntry( "cypher", "http://steveformayor.org/cypher" ) );
        assertThat( output, hasEntry( "relationship_types", "http://steveformayor.org/relationship/types" ) );
        assertThat( output, hasEntry( "relationship", "http://steveformayor.org/relationship" ) );
        assertThat( output, hasEntry( "transaction", "http://steveformayor.org/transaction" ) );
        assertThat( output, hasEntry( equalTo("neo4j_version"), notNullValue() ) );
    }
}
