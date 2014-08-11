/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.consistency.checking;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.index.lucene.LuceneLabelScanStoreBuilder;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.direct.DirectStoreAccess;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.impl.index.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreProvider;
import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TargetDirectory;

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singletonMap;

public abstract class GraphStoreFixture extends PageCacheRule implements TestRule
{
    private DirectStoreAccess directStoreAccess;

    public void apply( Transaction transaction ) throws TransactionFailureException
    {
        applyTransaction( transaction );
    }

    public DirectStoreAccess directStoreAccess()
    {
        if ( directStoreAccess == null )
        {
            DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
            PageCache pageCache = getPageCache( fileSystem, new Config() );
            StoreAccess nativeStores = new StoreAccess( fileSystem, pageCache, directory );
            directStoreAccess = new DirectStoreAccess(
                    nativeStores,
                    new LuceneLabelScanStoreBuilder(
                            directory().getAbsolutePath(),
                            nativeStores.getRawNeoStore(),
                            fileSystem,
                            StringLogger.SYSTEM
                    ).build(),
                    createIndexes()
            );
        }
        return directStoreAccess;
    }

    private SchemaIndexProvider createIndexes()
    {
        Config config = new Config( singletonMap( GraphDatabaseSettings.store_dir.name(),
                directory().getAbsolutePath() ) );
        return new LuceneSchemaIndexProvider( DirectoryFactory.PERSISTENT, config );
    }

    public File directory()
    {
        return new File( directory );
    }

    public static abstract class Transaction
    {
        public final long startTimestamp = currentTimeMillis();

        protected abstract void transactionData( TransactionDataBuilder tx, IdGenerator next );

        public TransactionRepresentation representation( IdGenerator idGenerator, int masterId, int authorId,
                long lastCommittedTx )
        {
            TransactionWriter writer = new TransactionWriter();
            transactionData( new TransactionDataBuilder( writer ), idGenerator );
            return writer.representation( new byte[0], masterId, authorId, startTimestamp, lastCommittedTx,
                   currentTimeMillis() );
        }
    }

    public IdGenerator idGenerator()
    {
        return new IdGenerator();
    }

    public class IdGenerator
    {
        public long schema()
        {
            return schemaId++;
        }

        public long node()
        {
            return nodeId++;
        }

        public int label()
        {
            return labelId++;
        }

        public long nodeLabel()
        {
            return nodeLabelsId++;
        }

        public long relationship()
        {
            return relId++;
        }

        public long relationshipGroup()
        {
            return relGroupId++;
        }

        public long property()
        {
            return propId++;
        }

        public long stringProperty()
        {
            return stringPropId++;
        }

        public long arrayProperty()
        {
            return arrayPropId++;
        }

        public int relationshipType()
        {
            return relTypeId++;
        }

        public int propertyKey()
        {
            return propKeyId++;
        }
    }

    public static final class TransactionDataBuilder
    {
        private final TransactionWriter writer;

        public TransactionDataBuilder( TransactionWriter writer )
        {
            this.writer = writer;
        }

        public void createSchema( Collection<DynamicRecord> beforeRecords, Collection<DynamicRecord> afterRecords,
                SchemaRule rule )
        {
            writer.createSchema( beforeRecords, afterRecords, rule );
        }

        public void propertyKey( int id, String key )
        {
            writer.propertyKey( id, key, id );
        }

        public void nodeLabel( int id, String name )
        {
            writer.label( id, name, id );
        }

        public void relationshipType( int id, String relationshipType )
        {
            writer.relationshipType( id, relationshipType, id );
        }

        public void create( NodeRecord node )
        {
            writer.create( node );
        }

        public void update( NeoStoreRecord record )
        {
            writer.update( record );
        }

        public void update( NodeRecord before, NodeRecord after )
        {
            writer.update( before, after );
        }

        public void delete( NodeRecord node )
        {
            writer.delete( node );
        }

        public void create( RelationshipRecord relationship )
        {
            writer.create( relationship );
        }

        public void update( RelationshipRecord relationship )
        {
            writer.update( relationship );
        }

        public void delete( RelationshipRecord relationship )
        {
            writer.delete( relationship );
        }

        public void create( RelationshipGroupRecord group )
        {
            writer.create( group );
        }

        public void update(  RelationshipGroupRecord group )
        {
            writer.update( group );
        }

        public void delete(  RelationshipGroupRecord group )
        {
            writer.delete( group );
        }

        public void create( PropertyRecord property )
        {
            writer.create( property );
        }

        public void update( PropertyRecord before, PropertyRecord property )
        {
            writer.update( before, property );
        }

        public void delete( PropertyRecord before, PropertyRecord property )
        {
            writer.delete( before, property );
        }

        private Error ioError( IOException e )
        {
            return new Error( "InMemoryLogBuffer should not throw IOException", e );
        }
    }

    protected abstract void generateInitialData( GraphDatabaseService graphDb );

    protected void start( @SuppressWarnings("UnusedParameters") String storeDir )
    {
        // allow for override
    }

    protected void stop() throws Throwable
    {
        if ( directStoreAccess != null )
        {
            directStoreAccess.close();
            directStoreAccess = null;
        }
    }

    protected int myId()
    {
        return 1;
    }

    protected int masterId()
    {
        return -1;
    }

    @SuppressWarnings("deprecation")
    protected void applyTransaction( Transaction transaction ) throws TransactionFailureException
    {
        // TODO you know... we could have just appended the transaction representation to the log
        // and the next startup of the store would do recovery where the transaction would have been
        // applied and all would have been well.

        GraphDatabaseAPI database = (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( directory ).setConfig( configuration( false ) ).newGraphDatabase();
        try
        {
            TransactionRepresentationCommitProcess commitProcess =
                    new TransactionRepresentationCommitProcess(
                            database.getDependencyResolver().resolveDependency( LogicalTransactionStore.class ),
                            database.getDependencyResolver().resolveDependency( KernelHealth.class ),
                            database.getDependencyResolver().resolveDependency( NeoStoreProvider.class ).evaluate(),
                            database.getDependencyResolver().resolveDependency( TransactionRepresentationStoreApplier.class ),
                            true /*recovery*/ );
            TransactionIdStore transactionIdStore = database.getDependencyResolver().resolveDependency(
                    TransactionIdStore.class );
            commitProcess.commit( transaction.representation( idGenerator(), masterId(), myId(),
                    transactionIdStore.getLastCommittedTransactionId() ) );
        }
        finally
        {
            database.shutdown();
        }
    }

    protected Map<String, String> configuration( boolean initialData )
    {
        return new HashMap<>();
    }

    private String directory;
    private long schemaId;
    private long nodeId;
    private int labelId;
    private long nodeLabelsId;
    private long relId;
    private long relGroupId;
    private int propId;
    private long stringPropId;
    private long arrayPropId;
    private int relTypeId;
    private int propKeyId;

    private void generateInitialData()
    {
        GraphDatabaseAPI graphDb = (GraphDatabaseAPI) new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( directory ).setConfig( configuration( true ) )
                .newGraphDatabase();
        try
        {
            generateInitialData( graphDb );
            StoreAccess stores = new StoreAccess( graphDb );
            schemaId = stores.getSchemaStore().getHighId();
            nodeId = stores.getNodeStore().getHighId();
            labelId = (int) stores.getLabelTokenStore().getHighId();
            nodeLabelsId = stores.getNodeDynamicLabelStore().getHighId();
            relId = stores.getRelationshipStore().getHighId();
            relGroupId = stores.getRelationshipGroupStore().getHighId();
            propId = (int) stores.getPropertyStore().getHighId();
            stringPropId = stores.getStringStore().getHighId();
            arrayPropId = stores.getArrayStore().getHighId();
            relTypeId = (int) stores.getRelationshipTypeTokenStore().getHighId();
            propKeyId = (int) stores.getPropertyKeyNameStore().getHighId();
        }
        finally
        {
            graphDb.shutdown();
        }
    }

    @Override
    public Statement apply( final Statement base, Description description )
    {
        final TargetDirectory.TestDirectory directory = TargetDirectory.forTest( description.getTestClass() )
                                                                       .testDirectory();
        return super.apply( directory.apply( new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                GraphStoreFixture.this.directory = directory.directory().getAbsolutePath();
                try
                {
                    generateInitialData();
                    start( GraphStoreFixture.this.directory );
                    try
                    {
                        base.evaluate();
                    }
                    finally
                    {
                        stop();
                    }
                }
                finally
                {
                    GraphStoreFixture.this.directory = null;
                }
            }
        }, description ), description );
    }
}
