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

import org.junit.jupiter.api.Test;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.common.TokenNameLookup.idTokenNameLookup;

class IndexPopulationFailedKernelExceptionTest
{
    private static final TokenNameLookup TOKEN_NAME_LOOKUP = idTokenNameLookup;

    @Test
    void shouldHandleMultiplePropertiesInConstructor1()
    {
        // Given
        LabelSchemaDescriptor descriptor = SchemaDescriptor.forLabel( 0, 42, 43, 44 );

        // When
        IndexPopulationFailedKernelException index = new IndexPopulationFailedKernelException(
                descriptor.userDescription( TOKEN_NAME_LOOKUP ), new RuntimeException() );

        // Then
        assertThat( index.getUserMessage( TOKEN_NAME_LOOKUP ) ).isEqualTo( "Failed to populate index :label[0](property[42], property[43], property[44])" );
    }

    @Test
    void shouldHandleMultiplePropertiesInConstructor2()
    {
        // Given
        LabelSchemaDescriptor descriptor = SchemaDescriptor.forLabel( 0, 42, 43, 44 );

        // When
        IndexPopulationFailedKernelException index = new IndexPopulationFailedKernelException(
                descriptor.userDescription( TOKEN_NAME_LOOKUP ), "an act of pure evil occurred" );

        // Then
        assertThat( index.getUserMessage( TOKEN_NAME_LOOKUP ) ).isEqualTo(
                "Failed to populate index :label[0](property[42], property[43], property[44]), due to an act of pure evil occurred" );
    }
}
