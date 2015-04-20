/*
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
package org.neo4j.kernel.ha;

import java.io.File;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;

import static java.util.Arrays.asList;

import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
import static org.neo4j.kernel.impl.util.StringLogger.DEFAULT_NAME;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsJoined;
import static org.neo4j.test.ha.ClusterManager.clusterWithAdditionalClients;
import static org.neo4j.test.ha.ClusterManager.masterAvailable;
import static org.neo4j.test.ha.ClusterManager.masterSeesMembers;

public class HaLoggingIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule(getClass());

    protected ClusterManager.ManagedCluster cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule
                  .provider( clusterWithAdditionalClients( 2, 1 ) )
                  .availabilityChecks( asList( masterAvailable(), masterSeesMembers( 3 ), allSeesAllAsJoined() ) )
                  .startCluster();
    }

    @Test
    public void logging_continues_after_role_switch() throws Exception
    {
        // GIVEN
        // -- look at the slave and see notices of startup diagnostics
        String logMessage = "Just a test for that logging continues as expected";
        HighlyAvailableGraphDatabase db = cluster.getAnySlave();
        StringLogger logger = db.getDependencyResolver().resolveDependency( StringLogger.class );
        logger.logMessage( logMessage, true );

        // WHEN
        // -- the slave switches role to become master, logging continues
        //    i.e. notices of startup diagnostics should be one more
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        cluster.shutdown( master );
        cluster.await( masterAvailable( master ) );
        cluster.await( masterSeesMembers( 2 ) );
        logger.logMessage( logMessage, true );

        // THEN
        int count = findLoggingLines( db, logMessage );
        Assert.assertEquals( 2, count );
    }

    private int findLoggingLines( HighlyAvailableGraphDatabase db, String toLookFor )
    {
        int count = 0;
        for ( String line : asIterable( new File( cluster.getStoreDir( db ), DEFAULT_NAME ), "UTF-8" ) )
            if ( line.endsWith( toLookFor ) )
                count++;
        return count;
    }
}
