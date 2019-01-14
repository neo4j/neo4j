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
package org.neo4j.kernel.impl.api.index;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class FailedIndexProxyTest
{
    private final IndexProvider.Descriptor providerDescriptor = mock( IndexProvider.Descriptor.class );
    private final IndexCapability indexCapability = mock( IndexCapability.class );
    private final IndexPopulator indexPopulator = mock( IndexPopulator.class );
    private final IndexPopulationFailure indexPopulationFailure = mock( IndexPopulationFailure.class );
    private final IndexCountsRemover indexCountsRemover = mock( IndexCountsRemover.class );

    @Test
    public void shouldRemoveIndexCountsWhenTheIndexItselfIsDropped() throws IOException
    {
        // given
        String userDescription = "description";
        FailedIndexProxy index = new FailedIndexProxy( indexMeta( SchemaIndexDescriptorFactory.forLabel( 1, 2 ) ),
                userDescription, indexPopulator, indexPopulationFailure, indexCountsRemover, NullLogProvider.getInstance() );

        // when
        index.drop();

        // then
        verify( indexPopulator ).drop();
        verify( indexCountsRemover ).remove();
        verifyNoMoreInteractions( indexPopulator, indexCountsRemover );
    }

    @Test
    public void shouldLogReasonForDroppingIndex() throws IOException
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();

        // when
        new FailedIndexProxy( indexMeta( SchemaIndexDescriptorFactory.forLabel( 0, 0 ) ),
                "foo", mock( IndexPopulator.class ), IndexPopulationFailure.failure( "it broke" ),
                indexCountsRemover, logProvider ).drop();

        // then
        logProvider.assertAtLeastOnce(
                inLog( FailedIndexProxy.class ).info( "FailedIndexProxy#drop index on foo dropped due to:\nit broke" )
        );
    }

    private IndexMeta indexMeta( SchemaIndexDescriptor descriptor )
    {
        return new IndexMeta( 1, descriptor, providerDescriptor, indexCapability );
    }
}
