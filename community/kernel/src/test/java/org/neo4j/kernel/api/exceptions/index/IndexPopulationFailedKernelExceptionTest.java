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
package org.neo4j.kernel.api.exceptions.index;

import org.junit.Test;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexPopulationFailedKernelExceptionTest
{

    @Test
    public void shouldHandleMultiplePropertiesInConstructor1()
    {
        // Given
        LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( 0, 42, 43, 44 );
        TokenNameLookup lookup = mock( TokenNameLookup.class );
        when( lookup.labelGetName( 0 ) ).thenReturn( "L" );
        when( lookup.propertyKeyGetName( 42 ) ).thenReturn( "FOO" );
        when( lookup.propertyKeyGetName( 43 ) ).thenReturn( "BAR" );
        when( lookup.propertyKeyGetName( 44 ) ).thenReturn( "BAZ" );

        // When
        IndexPopulationFailedKernelException index =
                new IndexPopulationFailedKernelException( descriptor, "INDEX", new RuntimeException() );

        // Then
        assertThat(index.getUserMessage( lookup ), equalTo(
                "Failed to populate index for INDEX [labelId: 0, properties [42, 43, 44]]"));
    }

    @Test
    public void shouldHandleMultiplePropertiesInConstructor2()
    {
        // Given
        LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( 0, 42, 43, 44 );
        TokenNameLookup lookup = mock( TokenNameLookup.class );
        when( lookup.labelGetName( 0 ) ).thenReturn( "L" );
        when( lookup.propertyKeyGetName( 42 ) ).thenReturn( "FOO" );
        when( lookup.propertyKeyGetName( 43 ) ).thenReturn( "BAR" );
        when( lookup.propertyKeyGetName( 44 ) ).thenReturn( "BAZ" );

        // When
        IndexPopulationFailedKernelException index =
                new IndexPopulationFailedKernelException( descriptor, "INDEX", "an act of pure evil occurred" );

        // Then
        assertThat(index.getUserMessage( lookup ), equalTo(
                "Failed to populate index for INDEX [labelId: 0, properties [42, 43, 44]], due to an act of pure evil occurred"));
    }
}
