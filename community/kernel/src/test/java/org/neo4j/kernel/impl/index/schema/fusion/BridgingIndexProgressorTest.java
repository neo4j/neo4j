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
package org.neo4j.kernel.impl.index.schema.fusion;

import org.junit.Test;

import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.storageengine.api.schema.IndexProgressor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BridgingIndexProgressorTest
{
    @Test
    public void closeMustCloseAll()
    {
        SchemaIndexDescriptor index = SchemaIndexDescriptorFactory.forLabel( 1, 2, 3 );
        BridgingIndexProgressor progressor = new BridgingIndexProgressor( null, index.schema().getPropertyIds() );

        IndexProgressor[] parts = {mock(IndexProgressor.class), mock(IndexProgressor.class)};

        // Given
        for ( IndexProgressor part : parts )
        {
            progressor.initialize( index, part, null );
        }

        // When
        progressor.close();

        // Then
        for ( IndexProgressor part : parts )
        {
            verify( part, times( 1 ) ).close();
        }
    }
}
