/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.test.ManagedResource;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertSame;

public class HaKernelDataTest
{
    @Test
    public void shouldReturnHaGraphDbFromKernelData() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase haGraphDb = ha.getResource();
        KernelData kernelData = haGraphDb.getDependencyResolver().resolveDependency( KernelData.class );

        // then
        assertSame( kernelData.graphDatabase(), haGraphDb );
    }

    @Rule
    public final ManagedResource<HighlyAvailableGraphDatabase> ha = new ManagedResource<HighlyAvailableGraphDatabase>()
    {
        @Override
        protected HighlyAvailableGraphDatabase createResource( TargetDirectory.TestDirectory dir ) throws Exception
        {
            return (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                    newHighlyAvailableDatabaseBuilder( dir.directory().getAbsolutePath() )
                    .setConfig( ClusterSettings.server_id, "1" )
                    .setConfig( ClusterSettings.initial_hosts, ":5001" )
                    .newGraphDatabase();
        }


        @Override
        protected void disposeResource( HighlyAvailableGraphDatabase resource )
        {
            resource.shutdown();
        }
    };
}
