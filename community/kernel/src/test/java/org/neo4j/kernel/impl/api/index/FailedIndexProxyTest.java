/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class FailedIndexProxyTest
{
    private final SchemaIndexProvider.Descriptor providerDescriptor = mock( SchemaIndexProvider.Descriptor.class );
    private final String userDescription = "description";
    private final IndexPopulator indexPopulator = mock( IndexPopulator.class );
    private final IndexPopulationFailure indexPopulationFailure = mock( IndexPopulationFailure.class );
    private final IndexCountsRemover indexCountsRemover = mock( IndexCountsRemover.class );

    @Test
    public void shouldRemoveIndexCountsWhenTheIndexItselfIsDropped() throws IOException
    {
        // given
        FailedIndexProxy index = new FailedIndexProxy(
                IndexDescriptorFactory.forLabel( 1, 2 ), providerDescriptor, userDescription,
                indexPopulator, indexPopulationFailure, indexCountsRemover, NullLogProvider.getInstance() );

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
        new FailedIndexProxy( IndexDescriptorFactory.forLabel( 0, 0 ),
                new SchemaIndexProvider.Descriptor( "foo", "bar" ), "foo",
                mock( IndexPopulator.class ), IndexPopulationFailure.failure( "it broke" ), indexCountsRemover,
                logProvider ).drop();

        // then
        logProvider.assertAtLeastOnce(
                inLog( FailedIndexProxy.class ).info( "FailedIndexProxy#drop index on foo dropped due to:\nit broke" )
        );
    }
}
