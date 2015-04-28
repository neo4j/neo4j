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
package org.neo4j.ha;

import java.io.File;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.com.ComException;
import org.neo4j.function.Function;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Predicates;
import org.neo4j.helpers.TransactionTemplate;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.test.OtherThreadRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.ha.ClusterManager;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.kernel.impl.api.integrationtest.UniquenessConstraintValidationConcurrencyIT.createNode;
import static org.neo4j.test.OtherThreadRule.isWaiting;
import static org.neo4j.test.TargetDirectory.testDirForTest;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;

public class UniquenessConstraintValidationHAIT
{
    public final @Rule LifeRule life = new LifeRule();
    public final @Rule TargetDirectory.TestDirectory targetDir =
            testDirForTest( UniquenessConstraintValidationHAIT.class );
    public final @Rule OtherThreadRule<Void> otherThread = new OtherThreadRule<>();

    @Before
    public void startLife()
    {
        life.start();
    }

    @Test
    public void shouldAllowCreationOfNonConflictingDataOnSeparateHosts() throws Exception
    {
        // given
        ClusterManager.ManagedCluster cluster = startClusterSeededWith(
                databaseWithUniquenessConstraint( "Label1", "key1" ) );

        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        HighlyAvailableGraphDatabase slave2 = cluster.getAnySlave( /*except:*/slave1 );

        // when
        Future<Boolean> created;
        Transaction tx = slave1.beginTx();
        try
        {
            slave1.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );

            created = otherThread.execute( createNode( slave2, "Label1", "key1", "value2" ) );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // then
        assertTrue( "creating non-conflicting data should pass", created.get() );
    }

    @Test
    public void shouldPreventConcurrentCreationOfConflictingDataOnSeparateHosts() throws Exception
    {
        // given
        ClusterManager.ManagedCluster cluster = startClusterSeededWith(
                databaseWithUniquenessConstraint( "Label1", "key1" ) );

        final HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        final HighlyAvailableGraphDatabase slave2 = cluster.getAnySlave( /*except:*/slave1 );

        // when
        TransactionTemplate template = new TransactionTemplate().
                retries( 3 ).
                backoff( 10, TimeUnit.SECONDS ).
                retryOn( Predicates.<Throwable>instanceOf( ComException.class ) ).
                with( slave1 );

        Future<Boolean> created = template.execute( new Function<Transaction, Future<Boolean>>()
        {
            @Override
            public Future<Boolean> apply( Transaction transaction ) throws RuntimeException
            {
                slave1.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );

                try
                {
                    return otherThread.execute( createNode( slave2, "Label1", "key1", "value1" ) );
                }
                finally
                {
                    assertThat( otherThread, isWaiting() );
                }
            }
        } );

        // then
        assertFalse( "creating violating data should fail", created.get() );
    }

    @Test
    public void shouldPreventConcurrentCreationOfConflictingNonStringPropertyOnMasterAndSlave() throws Exception
    {
        // given
        ClusterManager.ManagedCluster cluster = startClusterSeededWith(
                databaseWithUniquenessConstraint( "Label1", "key1" ) );

        HighlyAvailableGraphDatabase master = cluster.getMaster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        // when
        Future<Boolean> created;
        Transaction tx = master.beginTx();
        try
        {
            master.createNode( label( "Label1" ) ).setProperty( "key1", 0x0099CC );

            created = otherThread.execute( createNode( slave, "Label1", "key1", 0x0099CC ) );

            assertThat( otherThread, isWaiting() );

            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // then
        assertFalse( "creating violating data should fail", created.get() );
    }

    @Test
    public void shouldAllowOtherHostToCompleteIfFirstHostRollsBackTransaction() throws Exception
    {
        // given
        ClusterManager.ManagedCluster cluster = startClusterSeededWith(
                databaseWithUniquenessConstraint( "Label1", "key1" ) );

        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        HighlyAvailableGraphDatabase slave2 = cluster.getAnySlave( /*except:*/slave1 );

        // when
        Future<Boolean> created;
        Transaction tx = slave1.beginTx();
        try
        {
            slave1.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );

            created = otherThread.execute( createNode( slave2, "Label1", "key1", "value1" ) );

            assertThat( otherThread, isWaiting() );

            tx.failure();
        }
        finally
        {
            tx.finish();
        }

        // then
        assertTrue( "creating data that conflicts only with rolled back data should pass", created.get() );
    }

    private ClusterManager.ManagedCluster startClusterSeededWith( File seedDir )
    {
        ClusterManager.ManagedCluster cluster = life
                .add( new ClusterManager.Builder( targetDir.directory() ).withSeedDir( seedDir ).build() )
                .getDefaultCluster();
        cluster.await( allSeesAllAsAvailable() );
        return cluster;
    }

    private File databaseWithUniquenessConstraint( String label, String propertyKey )
    {
        File storeDir = new File( targetDir.directory(), "seed" );
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir.getAbsolutePath() );
        try
        {
            Transaction tx = graphDb.beginTx();
            try
            {
                graphDb.schema().constraintFor( label( label ) ).assertPropertyIsUnique( propertyKey ).create();

                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }
        finally
        {
            graphDb.shutdown();
        }
        return storeDir;
    }
}
