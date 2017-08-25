/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.ClassRule;

import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.impl.coreapi.IsolationLevel;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.ha.ClusterRule;
import org.neo4j.test.rule.DatabaseRule;

public class HASlaveToMasterIsolationLevelsTest extends IsolationLevelsTestTemplate
{
    @ClassRule
    public static ClusterRule clusterRule = new ClusterRule( HASlaveToMasterIsolationLevelsTest.class )
            .withSharedSetting( HaSettings.tx_push_factor, "2" );

    @Override
    protected DatabaseRule createDatabaseRule()
    {
        try
        {
            ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
            return new ClusterDatabaseRule( cluster );
        }
        catch ( Exception e )
        {
            throw new AssertionError( "Failed to start cluster", e );
        }
    }

    @Override
    protected int defaultTestRepetitions()
    {
        return 10;
    }

    @Override
    protected DbTask beginForked()
    {
        ClusterManager.ManagedCluster cluster = ((ClusterDatabaseRule) db).getCluster();
        return new DbTask( cluster.getMaster() );
    }

    private static class ClusterDatabaseRule extends DatabaseRule
    {
        private final ClusterManager.ManagedCluster cluster;

        private ClusterDatabaseRule( ClusterManager.ManagedCluster cluster )
        {
            this.cluster = cluster;
        }

        @Override
        protected GraphDatabaseFactory newFactory()
        {
            return null;
        }

        @Override
        protected GraphDatabaseBuilder newBuilder( GraphDatabaseFactory factory )
        {
            return null;
        }

        @Override
        protected GraphDatabaseAPI configureAndBuildDatabase( String... additionalConfig )
        {
            return cluster.getAnySlave();
        }

        ClusterManager.ManagedCluster getCluster()
        {
            return cluster;
        }
    }

    @Override
    public void settingIsolationLevelOnSchemaWriteTransactionMustThrow( IsolationLevel level )
    {
        // Ignore this one, because schema write transactions are not allowed on slaves in HA.
    }
}
