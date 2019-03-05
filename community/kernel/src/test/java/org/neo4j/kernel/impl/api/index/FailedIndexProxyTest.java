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

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProviderDescriptor;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.internal.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.impl.index.schema.IndexDescriptorFactory.forSchema;
import static org.neo4j.logging.AssertableLogProvider.inLog;

class FailedIndexProxyTest
{
    private final IndexPopulator indexPopulator = mock( IndexPopulator.class );
    private final IndexPopulationFailure indexPopulationFailure = mock( IndexPopulationFailure.class );
    private final IndexStatisticsStore indexStatisticsStore = mock( IndexStatisticsStore.class );

    @Test
    void shouldRemoveIndexCountsWhenTheIndexItselfIsDropped()
    {
        // given
        String userDescription = "description";
        FailedIndexProxy index =
                new FailedIndexProxy( forSchema( forLabel( 1, 2 ), IndexProviderDescriptor.UNDECIDED ).withId( 1 ).withoutCapabilities(),
                                      userDescription, indexPopulator, indexPopulationFailure, indexStatisticsStore, NullLogProvider.getInstance() );

        // when
        index.drop();

        // then
        verify( indexPopulator ).drop();
        verify( indexStatisticsStore ).removeIndex( anyLong() );
        verifyNoMoreInteractions( indexPopulator, indexStatisticsStore );
    }

    @Test
    void shouldLogReasonForDroppingIndex()
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();

        // when
        new FailedIndexProxy( forSchema( forLabel( 0, 0 ), IndexProviderDescriptor.UNDECIDED ).withId( 1 ).withoutCapabilities(),
                              "foo", mock( IndexPopulator.class ), IndexPopulationFailure.failure( "it broke" ),
                              indexStatisticsStore, logProvider ).drop();

        // then
        logProvider.assertAtLeastOnce(
                inLog( FailedIndexProxy.class ).info( "FailedIndexProxy#drop index on foo dropped due to:\nit broke" )
        );
    }
}
