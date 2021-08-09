/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.api.impl.schema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.index.schema.BuiltInDelegatingIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.fusion.NativeLuceneFusionIndexProviderFactory30;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.recovery.RecoveryExtension;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterators.asUniqueSet;

@ExtendWith( EphemeralFileSystemExtension.class )
class FusionIndexRecoveryIT
{
    public static final IndexProviderDescriptor DESCRIPTOR = new IndexProviderDescriptor( "initially-populating", "0.2" );

    private static final String NUM_BANANAS_KEY = "number_of_bananas_owned";
    private static final Label myLabel = label( "MyLabel" );

    @Inject
    private EphemeralFileSystemAbstraction fs;
    private GraphDatabaseAPI db;
    private DirectoryFactory directoryFactory;
    private DatabaseManagementService managementService;

    @BeforeEach
    void before()
    {
        directoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
    }

    @AfterEach
    void after() throws Exception
    {
        if ( db != null )
        {
            managementService.shutdown();
        }
        directoryFactory.close();
    }

    @Test
    void addShouldBeIdempotentWhenDoingRecovery()
    {
        // Given
        startDb();

        IndexDefinition index = createIndex( myLabel );
        waitForIndex( index );

        long nodeId = createNode( myLabel, "12" );
        try ( Transaction tx = db.beginTx() )
        {
            assertNotNull( tx.getNodeById( nodeId ) );
        }
        assertEquals( 1, doIndexLookup( myLabel, "12" ).size() );

        // And Given
        killDb();

        // When
        startDb();

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertNotNull( tx.getNodeById( nodeId ) );
        }
        assertEquals( 1, doIndexLookup( myLabel, "12" ).size() );
    }

    @Test
    void changeShouldBeIdempotentWhenDoingRecovery() throws Exception
    {
        // Given
        startDb();

        IndexDefinition indexDefinition = createIndex( myLabel );
        waitForIndex( indexDefinition );

        long node = createNode( myLabel, "12" );
        rotateLogsAndCheckPoint();

        updateNode( node, "13" );

        // And Given
        killDb();

        // When
        startDb();

        // Then
        assertEquals( 0, doIndexLookup( myLabel, "12" ).size() );
        assertEquals( 1, doIndexLookup( myLabel, "13" ).size() );
    }

    @Test
    void removeShouldBeIdempotentWhenDoingRecovery() throws Exception
    {
        // Given
        startDb();

        IndexDefinition indexDefinition = createIndex( myLabel );
        waitForIndex( indexDefinition );

        long node = createNode( myLabel, "12" );
        rotateLogsAndCheckPoint();

        deleteNode( node );

        // And Given
        killDb();

        // When
        startDb();

        // Then
        assertEquals( 0, doIndexLookup( myLabel, "12" ).size() );
    }

    @Test
    void shouldNotAddTwiceDuringRecoveryIfCrashedDuringPopulation()
    {
        // Given
        startDb( createAlwaysInitiallyPopulatingLuceneIndexFactory() );

        IndexDefinition indexDefinition = createIndex( myLabel );
        waitForIndex( indexDefinition );

        long nodeId = createNode( myLabel, "12" );
        assertEquals( 1, doIndexLookup( myLabel, "12" ).size() );

        // And Given
        killDb();

        // When
        startDb( createAlwaysInitiallyPopulatingLuceneIndexFactory() );

        IndexDefinition index;
        try ( Transaction tx = db.beginTx() )
        {
            index = tx.schema().getIndexes().iterator().next();
        }
        waitForIndex( index );

        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( "12", tx.getNodeById( nodeId ).getProperty( NUM_BANANAS_KEY ) );
        }
        assertEquals( 1, doIndexLookup( myLabel, "12" ).size() );
    }

    @Test
    void shouldNotUpdateTwiceDuringRecovery()
    {
        // Given
        startDb();

        IndexDefinition indexDefinition = createIndex( myLabel );
        waitForIndex( indexDefinition );

        long nodeId = createNode( myLabel, "12" );
        updateNode( nodeId, "14" );

        // And Given
        killDb();

        // When
        startDb();

        // Then
        assertEquals( 0, doIndexLookup( myLabel, "12" ).size() );
        assertEquals( 1, doIndexLookup( myLabel, "14" ).size() );
    }

    private void startDb()
    {
        startDb( null );
    }

    private void startDb( ExtensionFactory<?> indexProviderFactory )
    {
        if ( db != null )
        {
            managementService.shutdown();
        }

        TestDatabaseManagementServiceBuilder factory = new TestDatabaseManagementServiceBuilder();
        factory.setFileSystem( fs ).impermanent();
        if ( indexProviderFactory != null )
        {
            factory.addExtension( indexProviderFactory )
                   .setConfig( default_schema_provider, DESCRIPTOR.name() );
        }
        else
        {
            factory.setConfig( default_schema_provider, NativeLuceneFusionIndexProviderFactory30.DESCRIPTOR.name() );
        }
        managementService = factory.build();
        db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    private void killDb()
    {
        if ( db != null )
        {
            fs = fs.snapshot();
            managementService.shutdown();
        }
    }

    private void rotateLogsAndCheckPoint() throws IOException
    {
        DependencyResolver resolver = db.getDependencyResolver();
        resolver.resolveDependency( LogRotation.class ).rotateLogFile( LogAppendEvent.NULL );
        resolver.resolveDependency( CheckPointer.class ).forceCheckPoint( new SimpleTriggerInfo( "test" ) );
    }

    private IndexDefinition createIndex( Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition definition = tx.schema().indexFor( label ).on( NUM_BANANAS_KEY ).create();
            tx.commit();
            return definition;
        }
    }

    private void waitForIndex( IndexDefinition definition )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( definition, 10, TimeUnit.MINUTES );
            tx.commit();
        }
    }

    private Set<Node> doIndexLookup( Label myLabel, String value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Iterator<Node> iter = tx.findNodes( myLabel, NUM_BANANAS_KEY, value );
            Set<Node> nodes = asUniqueSet( iter );
            tx.commit();
            return nodes;
        }
    }

    private long createNode( Label label, String number )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( label );
            node.setProperty( NUM_BANANAS_KEY, number );
            tx.commit();
            return node.getId();
        }
    }

    private void updateNode( long nodeId, String value )
    {

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.getNodeById( nodeId );
            node.setProperty( NUM_BANANAS_KEY, value );
            tx.commit();
        }
    }

    private void deleteNode( long node )
    {

        try ( Transaction tx = db.beginTx() )
        {
            tx.getNodeById( node ).delete();
            tx.commit();
        }
    }

    private ExtensionFactory<?> createAlwaysInitiallyPopulatingLuceneIndexFactory()
    {
        return new InitiallyPopulatingExtension();
    }

    @RecoveryExtension
    private static class InitiallyPopulatingExtension extends BuiltInDelegatingIndexProviderFactory
    {
        InitiallyPopulatingExtension()
        {
            super( new NativeLuceneFusionIndexProviderFactory30(), DESCRIPTOR );
        }

        @Override
        public IndexProvider newInstance( ExtensionContext context, Dependencies dependencies )
        {
            var actualProvider = super.newInstance( context, dependencies );
            return new IndexProvider.Delegating( actualProvider )
            {
                @Override
                public InternalIndexState getInitialState( IndexDescriptor descriptor, CursorContext cursorContext )
                {
                    return InternalIndexState.POPULATING;
                }
            };
        }
    }

}
