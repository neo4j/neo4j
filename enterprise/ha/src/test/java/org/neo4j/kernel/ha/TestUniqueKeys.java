/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.test.AbstractClusterTest;
import org.neo4j.test.ha.ClusterManager.ManagedCluster;

import static org.junit.Assert.assertTrue;
import static org.neo4j.test.ha.ClusterManager.masterAvailable;

public class TestUniqueKeys extends AbstractClusterTest
{
    @Test
    public void bruteForceCreateSameRelationshipTypeOnDifferentSlaveAtTheSameTimeShouldYieldSameId() throws Exception
    {
        // Get a hold of all the slaves in there
        List<HighlyAvailableGraphDatabase> slaves = new ArrayList<HighlyAvailableGraphDatabase>();
        for ( int i = 0; i < cluster.size()-1; i++ )
            slaves.add( cluster.getAnySlave( slaves.toArray( new HighlyAvailableGraphDatabase[0] ) ) );
        
        for ( int i = 0; i < 10; i++ )
        {
            final RelationshipType relType = DynamicRelationshipType.withName( "Rel" + i );
            final CountDownLatch latch = new CountDownLatch( 1 );
            List<Thread> threads = new ArrayList<Thread>();
            for ( HighlyAvailableGraphDatabase slave : slaves )
            {
                final GraphDatabaseAPI db = slave;
                Thread thread = new Thread()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            latch.await();
                        }
                        catch ( InterruptedException e )
                        {
                            throw new RuntimeException( e );
                        }

                        Transaction tx = db.beginTx();
                        try
                        {
                            db.createNode().createRelationshipTo( db.createNode(), relType );
                            tx.success();
                        }
                        finally
                        {
                            tx.finish();
                        }
                    }
                };
                thread.start();
                threads.add( thread );
            }

            latch.countDown();
            for ( Thread thread : threads )
            {
                thread.join();
            }

            // Verify so that the relationship type on all the machines has got the same id
            int highestId = 0;
            for ( GraphDatabaseAPI db : cluster.getAllMembers() )
            {
                RelationshipTypeTokenHolder holder = db.getRelationshipTypeTokenHolder();
                highestId = highestIdOf( holder, highestId );
                Set<String> types = new HashSet<String>();
                for ( int j = 0; j <= highestId; j++ )
                {
                    RelationshipType type = holder.getTokenById( j );
                    if ( type != null )
                    {
                        assertTrue( type.name() + " already existed for " + db, types.add( type.name() ) );
                    }
                }
            }
        }
    }
    
    @Test
    public void bruteForceCreateSamePropertyKeyOnDifferentSlaveAtTheSameTimeShouldYieldSameId() throws Exception
    {
        // Get a hold of all the slaves in there
        List<HighlyAvailableGraphDatabase> slaves = new ArrayList<HighlyAvailableGraphDatabase>();
        for ( int i = 0; i < cluster.size()-1; i++ )
            slaves.add( cluster.getAnySlave( slaves.toArray( new HighlyAvailableGraphDatabase[0] ) ) );
        
        for ( int i = 0; i < 10; i++ )
        {
            final String key = "Key" + i;
            final CountDownLatch latch = new CountDownLatch( 1 );
            List<Thread> threads = new ArrayList<Thread>();
            for ( HighlyAvailableGraphDatabase slave : slaves )
            {
                final GraphDatabaseAPI db = slave;
                Thread thread = new Thread()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            latch.await();
                        }
                        catch ( InterruptedException e )
                        {
                            throw new RuntimeException( e );
                        }

                        Transaction tx = db.beginTx();
                        try
                        {
                            db.createNode().setProperty( key, "yes" );
                            tx.success();
                        }
                        finally
                        {
                            tx.finish();
                        }
                    }
                };
                thread.start();
                threads.add( thread );
            }

            latch.countDown();
            for ( Thread thread : threads )
            {
                thread.join();
            }

            // Verify so that the property keys on all the machines has got the same id
            int highestId = 0;
            for ( GraphDatabaseAPI db : cluster.getAllMembers() )
            {
                TokenHolder<Token> holder = db.getDependencyResolver().resolveDependency( PropertyKeyTokenHolder.class );
                highestId = highestIdOf( holder, highestId );
                Set<String> types = new HashSet<String>();
                for ( int j = 0; j <= highestId; j++ )
                {
                    Token type = holder.getTokenById( j );
                    if ( type != null )
                    {
                        assertTrue( type.name() + " already existed for " + db, types.add( type.name() ) );
                    }
                }
            }
        }
    }
    
    private ManagedCluster cluster;

    @Before
    public void getCluster() throws Exception
    {
        cluster = clusterManager.getDefaultCluster();
        cluster.await( masterAvailable() );
    }

    private <KEY extends Token> int highestIdOf( TokenHolder<KEY> holder, int high ) throws TokenNotFoundException
    {
        for ( Token type : holder.getAllTokens() )
            high = Math.max( type.id(), high );
        return high;
    }
}
