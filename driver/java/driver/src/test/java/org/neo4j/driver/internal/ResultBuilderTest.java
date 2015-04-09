/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.internal;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.driver.Record;
import org.neo4j.driver.ReusableResult;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.ClientException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.driver.Values.value;

public class ResultBuilderTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldBuildHappyPathResult()
    {
        // Given
        ResultBuilder builder = new ResultBuilder();
        builder.fieldNames( new String[]{"a"} );
        builder.record( new Value[]{value( "Admin" )} );

        // When
        ReusableResult result = builder.build().retain();

        // Then
        assertThat( result.size(), equalTo( 1l ) );

        Record record = result.get( 0 );
        assertThat( record.get( 0 ).javaString(), equalTo( "Admin" ) );
    }

    @Test
    public void shouldHandleEmptyTable()
    {
        // Given
        ResultBuilder builder = new ResultBuilder();

        // When
        ReusableResult result = builder.build().retain();

        // Then
        assertThat( result.size(), equalTo( 0l ) );
    }

    @Test
    public void shouldThrowNoSuchSomething()
    {
        // Given
        ResultBuilder builder = new ResultBuilder();
        builder.fieldNames( new String[]{"a"} );
        builder.record( new Value[]{value( "Admin" )} );

        ReusableResult result = builder.build().retain();

        // Expect
        exception.expect( ClientException.class );

        // When
        result.get( 2 );
    }
}