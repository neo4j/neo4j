/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingController;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.index_background_sampling_enabled;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.register.Registers.newDoubleLongRegister;

public class IndexStatisticsIT
{
    public static final Label ALIEN = label( "Alien" );
    public static final String SPECIMEN = "specimen";

    // NOTE: Index sampling is disabled in this test

    @Test
    public void shouldRecoverIndexCountsBySamplingThemOnStartup()
    {
        // given some aliens in a database
        createAliens();

        // that have been indexed
        awaitIndexOnline( indexAliensBySpecimen() );

        // where ALIEN and SPECIMEN are both the first ids of their kind
        int labelId = labelId( ALIEN );
        int pkId = pkId( SPECIMEN );

        // for which we don't have index counts
        resetIndexCounts( labelId, pkId );

        // when we shutdown the database and restart it
        restart();

        // then we should have re-sampled the index
        CountsTracker tracker = neoStores().getCounts();
        assertEqualRegisters(
                "Unexpected updates and size for the index",
                newDoubleLongRegister( 0, 32 ),
                tracker.indexUpdatesAndSize( labelId, pkId, newDoubleLongRegister() ) );
        assertEqualRegisters(
            "Unexpected sampling result",
            newDoubleLongRegister( 16, 32 ),
            tracker.indexSample( labelId, pkId, newDoubleLongRegister() )
        );

        // and also
        assertLogExistsForRecoveryOn( ":Alien(specimen)" );
    }

    private void assertEqualRegisters( String message, DoubleLongRegister expected, DoubleLongRegister actual )
    {
        assertEquals( message + " (first part of register)", expected.readFirst(), actual.readFirst() );
        assertEquals( message + " (second part of register)", expected.readSecond(), actual.readSecond() );
    }

    private void assertLogExistsForRecoveryOn( String labelAndProperty )
    {
        logProvider.assertAtLeastOnce(
                inLog( IndexSamplingController.class ).warn( "Recovering index sampling for index %s", labelAndProperty )
        );
    }

    private int labelId( Label alien )
    {
        try ( Transaction ignore = db.beginTx() )
        {
            return statement().readOperations().labelGetForName( alien.name() );
        }
    }

    private int pkId( String propertyName )
    {
        try ( Transaction ignore = db.beginTx() )
        {
            return statement().readOperations().propertyKeyGetForName( propertyName );
        }
    }

    private Statement statement()
    {
        return ( (GraphDatabaseAPI) db ).getDependencyResolver()
                                        .resolveDependency( ThreadToStatementContextBridge.class )
                                        .get();
    }

    private void createAliens()
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 32; i++ )
            {
                Node alien = db.createNode( ALIEN );
                alien.setProperty( SPECIMEN, i / 2 );
            }
            tx.success();
        }
    }

    private void awaitIndexOnline( IndexDefinition definition )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexOnline( definition, 10, TimeUnit.SECONDS );
            tx.success();
        }
    }

    private IndexDefinition indexAliensBySpecimen()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition definition = db.schema().indexFor( ALIEN ).on( SPECIMEN ).create();
            tx.success();
            return definition;
        }
    }

    private void resetIndexCounts( int labelId, int pkId )
    {
        try ( CountsAccessor.IndexStatsUpdater updater = neoStores().getCounts().updateIndexCounts() )
        {
            updater.replaceIndexSample( labelId, pkId, 0, 0 );
            updater.replaceIndexUpdateAndSize( labelId, pkId, 0, 0 );
        }
    }

    private NeoStores neoStores()
    {
        return ( (GraphDatabaseAPI) db ).getDependencyResolver().resolveDependency( NeoStores.class );
    }

    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final InMemoryIndexProvider indexProvider = new InMemoryIndexProvider( 100 );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private GraphDatabaseService db;

    @Before
    public void before()
    {
        setupDb( fsRule.get() );
    }

    private void setupDb( EphemeralFileSystemAbstraction fs )
    {
        db = new TestGraphDatabaseFactory().setInternalLogProvider( logProvider )
                                           .setFileSystem( fs )
                                           .addKernelExtension( new InMemoryIndexProviderFactory( indexProvider ) )
                                           .newImpermanentDatabaseBuilder()
                                           .setConfig( index_background_sampling_enabled, "false" )
                                           .newGraphDatabase();
    }

    public void restart()
    {
        db.shutdown();
        setupDb( fsRule.get().snapshot() );
    }

    @After
    public void after()
    {
        try
        {
            db.shutdown();
        }
        finally
        {
            db = null;
        }
    }
}
