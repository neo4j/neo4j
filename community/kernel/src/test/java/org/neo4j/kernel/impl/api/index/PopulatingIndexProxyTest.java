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
package org.neo4j.kernel.impl.api.index;

import org.junit.Test;

import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreparedIndexUpdates;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.impl.util.JobScheduler;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.add;

public class PopulatingIndexProxyTest
{
    @Test
    public void preparedIndexUpdatesShouldCommitChangesToPopulationJobQueue() throws Exception
    {
        // Given
        int labelId = 42;
        int propertyKeyId = 42;
        IndexDescriptor indexDescriptor = new IndexDescriptor( labelId, propertyKeyId );
        IndexPopulationJob indexPopulationJob = mock( IndexPopulationJob.class );

        PopulatingIndexProxy indexProxy = new PopulatingIndexProxy( mock( JobScheduler.class ), indexDescriptor,
                new SchemaIndexProvider.Descriptor( "key", "value" ), indexPopulationJob );

        IndexUpdater updater = indexProxy.newUpdater( IndexUpdateMode.ONLINE );

        NodePropertyUpdate update1 = add( 1, propertyKeyId, "foo", new long[]{labelId} );
        NodePropertyUpdate update2 = add( 2, propertyKeyId, "bar", new long[]{labelId} );

        PreparedIndexUpdates updates = updater.prepare( asList( update1, update2 ) );

        verifyZeroInteractions( indexPopulationJob );

        // When
        updates.commit();

        // Then
        verify( indexPopulationJob ).update( update1 );
        verify( indexPopulationJob ).update( update2 );
    }
}
