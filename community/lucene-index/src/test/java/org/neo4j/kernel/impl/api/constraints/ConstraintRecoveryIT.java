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
package org.neo4j.kernel.impl.api.constraints;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConstraintRecoveryIT
{
    private static final Label LABEL = DynamicLabel.label( "label1" );
    @Rule
    public EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();
    private GraphDatabaseAPI db;

    @Test
    public void shouldNotHaveAnIndexIfUniqueConstraintCreationOnRecoveryFails() throws IOException
    {
        // given
        final EphemeralFileSystemAbstraction fs = fileSystemRule.get();
        fs.mkdir( new File("/tmp") );
        File pathToDb = new File( "/tmp/bar2" );

        TestGraphDatabaseFactory dbFactory = new TestGraphDatabaseFactory();
        dbFactory.setFileSystem( fs );

        final EphemeralFileSystemAbstraction[] storeInNeedOfRecovery = new EphemeralFileSystemAbstraction[1];
        final AtomicBoolean monitorCalled = new AtomicBoolean( false );

        Monitors monitors = new Monitors();
        monitors.addMonitorListener( new IndexingService.MonitorAdapter()
        {
            @Override
            public void verifyDeferredConstraints()
            {
                monitorCalled.set( true );
                db.getDependencyResolver().resolveDependency( NeoStoresSupplier.class ).get().getSchemaStore().flush();
                storeInNeedOfRecovery[0] = fs.snapshot();
            }
        } );
        dbFactory.setMonitors( monitors );


        db = (GraphDatabaseAPI) dbFactory.newImpermanentDatabase( pathToDb );

        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 2; i++ )
            {
                Node node1 = db.createNode( LABEL );
                node1.setProperty( "prop", true );
            }

            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( LABEL ).assertPropertyIsUnique( "prop" ).create();
            fail("Should have failed with ConstraintViolationException");
            tx.success();
        }
        catch ( ConstraintViolationException ignored )  { }

        db.shutdown();

        assertTrue( monitorCalled.get() );

        // when
        dbFactory = new TestGraphDatabaseFactory();
        dbFactory.setFileSystem( storeInNeedOfRecovery[0] );
        db = (GraphDatabaseAPI) dbFactory.newImpermanentDatabase( pathToDb );

        // then
        try(Transaction tx = db.beginTx())
        {
            db.schema().awaitIndexesOnline( 5000, TimeUnit.MILLISECONDS );
        }

        try(Transaction tx = db.beginTx())
        {
            assertEquals(2, Iterables.count( GlobalGraphOperations.at( db ).getAllNodes() ) );
        }

        try(Transaction tx = db.beginTx())
        {
            assertEquals(0, Iterables.count(Iterables.toList( db.schema().getConstraints() )));
        }

        try(Transaction tx = db.beginTx())
        {
            assertEquals(0, Iterables.count(Iterables.toList( db.schema().getIndexes() )));
        }

        db.shutdown();
    }
}
