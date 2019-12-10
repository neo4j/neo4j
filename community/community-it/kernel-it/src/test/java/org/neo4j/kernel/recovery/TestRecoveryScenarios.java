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
package org.neo4j.kernel.recovery;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Collections;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.kernel.api.KernelTransaction.Type.EXPLICIT;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;

@ExtendWith( EphemeralFileSystemExtension.class )
class TestRecoveryScenarios
{
    @Inject
    private EphemeralFileSystemAbstraction fs;

    private final Label label = label( "label" );
    private GraphDatabaseAPI db;

    private DatabaseManagementService managementService;

    @BeforeEach
    void before()
    {
        managementService = databaseFactory( fs ).impermanent().build();
        db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void after()
    {
        managementService.shutdown();
    }

    @ParameterizedTest
    @EnumSource( FlushStrategy.class )
    void shouldRecoverTransactionWhereNodeIsDeletedInTheFuture( FlushStrategy strategy ) throws Exception
    {
        // GIVEN
        Node node = createNodeWithProperty( "key", "value", label );
        checkPoint();
        setProperty( node, "other-key", 1 );
        deleteNode( node );
        strategy.flush( db );

        // WHEN
        crashAndRestart();

        // THEN
        // -- really the problem was that recovery threw exception, so mostly assert that.
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.getNodeById( node.getId() );
            tx.commit();
            fail( "Should not exist" );
        }
        catch ( NotFoundException e )
        {
            assertEquals( "Node " + node.getId() + " not found", e.getMessage() );
        }
    }

    @ParameterizedTest
    @EnumSource( FlushStrategy.class )
    void shouldRecoverTransactionWherePropertyIsRemovedInTheFuture( FlushStrategy strategy ) throws Exception
    {
        // GIVEN
        createIndex( label, "key" );
        Node node = createNodeWithProperty( "key", "value" );
        checkPoint();
        addLabel( node, label );
        removeProperty( node, "key" );
        strategy.flush( db );

        // WHEN
        crashAndRestart();

        // THEN
        // -- really the problem was that recovery threw exception, so mostly assert that.
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( Collections.<Node>emptyList(),
                Iterators.asList( tx.findNodes( label, "key", "value" ) ), "Updates not propagated correctly during recovery" );
            tx.commit();
        }
    }

    @ParameterizedTest
    @EnumSource( FlushStrategy.class )
    void shouldRecoverTransactionWhereManyLabelsAreRemovedInTheFuture( FlushStrategy strategy ) throws Exception
    {
        // GIVEN
        createIndex( label, "key" );
        Label[] labels = new Label[16];
        for ( int i = 0; i < labels.length; i++ )
        {
            labels[i] = label( "Label" + Integer.toHexString( i ) );
        }
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = tx.createNode( labels );
            node.addLabel( label );
            tx.commit();
        }
        checkPoint();
        setProperty( node, "key", "value" );
        removeLabels( node, labels );
        strategy.flush( db );

        // WHEN
        crashAndRestart();

        // THEN
        // -- really the problem was that recovery threw exception, so mostly assert that.
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( node, tx.findNode( label, "key", "value" ) );
            tx.commit();
        }
    }

    @Test
    void shouldRecoverCounts() throws Exception
    {
        // GIVEN
        Node node = createNode( label );
        checkPoint();
        deleteNode( node );

        // WHEN
        crashAndRestart();

        // THEN
        // -- really the problem was that recovery threw exception, so mostly assert that.
        Kernel kernel = db.getDependencyResolver().resolveDependency( Kernel.class );
        try ( KernelTransaction tx = kernel.beginTransaction( EXPLICIT, LoginContext.AUTH_DISABLED ) )
        {
            assertEquals( 0, tx.dataRead().countsForNode( ANY_LABEL ) );
            final TokenHolder holder = db.getDependencyResolver().resolveDependency( TokenHolders.class ).labelTokens();
            int labelId = holder.getIdByName( label.name() );
            assertEquals( 0, tx.dataRead().countsForNode( labelId ) );
            tx.commit();
        }
    }

    private void removeLabels( Node node, Label... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            var targetNode = tx.getNodeById( node.getId() );
            for ( Label label : labels )
            {
                targetNode.removeLabel( label );
            }
            tx.commit();
        }
    }

    private void removeProperty( Node node, String key )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( node.getId() ).removeProperty( key );
            tx.commit();
        }
    }

    private void addLabel( Node node, Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( node.getId() ).addLabel( label );
            tx.commit();
        }
    }

    private Node createNode( Label... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( labels );
            tx.commit();
            return node;
        }
    }

    private Node createNodeWithProperty( String key, String value, Label... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( labels );
            node.setProperty( key, value );
            tx.commit();
            return node;
        }
    }

    private void createIndex( Label label, String key )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( label ).on( key ).create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 10, SECONDS );
            tx.commit();
        }
    }

    public enum FlushStrategy
    {
        FORCE_EVERYTHING
                {
                    @Override
                    void flush( GraphDatabaseAPI db ) throws IOException
                    {
                        IOLimiter limiter = IOLimiter.UNLIMITED;
                        db.getDependencyResolver().resolveDependency( CheckPointerImpl.ForceOperation.class ).flushAndForce( limiter );
                    }
                },
        FLUSH_PAGE_CACHE
                {
                    @Override
                    void flush( GraphDatabaseAPI db ) throws IOException
                    {
                        db.getDependencyResolver().resolveDependency( PageCache.class ).flushAndForce();
                    }
                };

        abstract void flush( GraphDatabaseAPI db ) throws IOException;
    }

    private void checkPoint() throws IOException
    {
        db.getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint(
                new SimpleTriggerInfo( "test" )
        );
    }

    private void deleteNode( Node node )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( node.getId() ).delete();
            tx.commit();
        }
    }

    private void setProperty( Node node, String key, Object value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( node.getId() ).setProperty( key, value );
            tx.commit();
        }
    }

    private static TestDatabaseManagementServiceBuilder databaseFactory( FileSystemAbstraction fs )
    {
        return new TestDatabaseManagementServiceBuilder().setFileSystem( fs );
    }

    private void crashAndRestart() throws Exception
    {
        var uncleanFs = fs.snapshot();
        try
        {
            managementService.shutdown();
        }
        finally
        {
            fs.close();
            fs = uncleanFs;
        }

        managementService = databaseFactory( uncleanFs ).impermanent().build();
        db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }
}
