/*
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
package org.neo4j.ndp.runtime.internal;

import org.junit.Test;

import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.MapUtil;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.runtime.internal.runner.StreamMatchers.equalsStream;
import static org.neo4j.runtime.internal.runner.StreamMatchers.eqRecord;

public class CypherCursorAdapterTest
{
    @Test
    @SuppressWarnings("unchecked")
    public void nextShouldNotEqualNotNext()
    {
        // Given
        Result result = mock( Result.class );
        when( result.columns() ).thenReturn( asList( "name" ) );
        when( result.hasNext() ).thenReturn( true, true, false );
        when( result.next() ).thenReturn( MapUtil.map( "name", "bob" ), MapUtil.map( "name", "Steve Brook" ), null );

        // When
        CypherAdapterStream cursor = new CypherAdapterStream( result );

        // Then
        assertThat( cursor, equalsStream(
                new String[]{"name"},
                eqRecord( equalTo( "bob" ) ),
                eqRecord( equalTo( "Steve Brook" ) )) );
    }
}