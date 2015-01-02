/**
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
package org.neo4j.consistency.checking;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.index.lucene.LuceneLabelScanStoreBuilder;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.direct.DirectStoreAccess;
import org.neo4j.kernel.api.impl.index.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.nioneo.xa.TransactionWriter;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.transaction.xaframework.InMemoryLogBuffer;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.TargetDirectory;

import static java.util.Collections.singletonMap;

import static org.neo4j.kernel.impl.transaction.XidImpl.DEFAULT_SEED;

public abstract class GraphStoreFixture implements TestRule
{
    private DirectStoreAccess directStoreAccess;

    public void apply( Transaction transaction ) throws IOException
    {
        applyTransaction( transaction );
    }

    public DirectStoreAccess directStoreAccess()
    {
        if ( directStoreAccess == null )
        {
            StoreAccess nativeStores = new StoreAccess( directory );
            directStoreAccess = new DirectStoreAccess(
                    nativeStores,
                    new LuceneLabelScanStoreBuilder(
                            directory().getAbsolutePath(),
                            nativeStores.getRawNeoStore(),
                            new DefaultFileSystemAbstraction(),
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
        public final long startTimestamp = System.currentTimeMillis();
        public final byte[] globalId = XidImpl.getNewGlobalId( DEFAULT_SEED, -1 );

        protected abstract void transactionData( TransactionDataBuilder tx, IdGenerator next );

        private ReadableByteChannel write( IdGenerator idGenerator, int identifier, int masterId, int myId, long txId )
                throws IOException
        {
            InMemoryLogBuffer buffer = new InMemoryLogBuffer();
            TransactionWriter writer = new TransactionWriter( buffer, identifier, myId );
            writer.start( globalId, masterId, myId, startTimestamp, txId );

            transactionData( new TransactionDataBuilder( writer ), idGenerator );

            writer.prepare();
            return buffer;
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

        public void createSchema( Collection<DynamicRecord> beforeRecords, Collection<DynamicRecord> afterRecords )
        {
            try
            {
                writer.createSchema( beforeRecords, afterRecords );
            }
            catch ( IOException e )
            {
                throw ioError( e );
            }
        }

        public void propertyKey( int id, String key )
        {
            try
            {
                writer.propertyKey( id, key, id );
            }
            catch ( IOException e )
            {
                throw ioError( e );
            }
        }

        public void nodeLabel( int id, String name )
        {
            try
            {
                writer.label( id, name, id );
            }
            catch ( IOException e )
            {
                throw ioError( e );
            }
        }

        public void relationshipType( int id, String relationshipType )
        {
            try
            {
                writer.relationshipType( id, relationshipType, id );
            }
            catch ( IOException e )
            {
                throw ioError( e );
            }
        }

        public void create( NodeRecord node )
        {
            try
            {
                writer.create( node );
            }
            catch ( IOException e )
            {
                throw ioError( e );
            }
        }

        public void update( NeoStoreRecord record )
        {
            try
            {
                writer.update( record );
            }
            catch ( IOException e )
            {
                throw ioError( e );
            }
        }

        public void update( NodeRecord before, NodeRecord after )
        {
            try
            {
                writer.update( before, after );
            }
            catch ( IOException e )
            {
                throw ioError( e );
            }
        }

        public void delete( NodeRecord node )
        {
            try
            {
                writer.delete( node );
            }
            catch ( IOException e )
            {
                throw ioError( e );
            }
        }

        public void create( RelationshipRecord relationship )
        {
            try
            {
                writer.create( relationship );
            }
            catch ( IOException e )
            {
                throw ioError( e );
            }
        }

        public void update( RelationshipRecord relationship )
        {
            try
            {
                writer.update( relationship );
            }
            catch ( IOException e )
            {
                throw ioError( e );
            }
        }

        public void delete( RelationshipRecord relationship )
        {
            try
            {
                writer.delete( relationship );
            }
            catch ( IOException e )
            {
                throw ioError( e );
            }
        }

        public void create( PropertyRecord property )
        {
            try
            {
                writer.create( property );
            }
            catch ( IOException e )
            {
                throw ioError( e );
            }
        }

        public void update( PropertyRecord before, PropertyRecord property )
        {
            try
            {
                writer.update( before, property );
            }
            catch ( IOException e )
            {
                throw ioError( e );
            }
        }

        public void delete( PropertyRecord before, PropertyRecord property )
        {
            try
            {
                writer.delete( before, property );
            }
            catch ( IOException e )
            {
                throw ioError( e );
            }
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

    protected final ReadableByteChannel write( Transaction transaction, Long txId ) throws IOException
    {
        return transaction.write( idGenerator(), localIdGenerator++, masterId(), myId(), txId );
    }

    @SuppressWarnings("deprecation")
    protected void applyTransaction( Transaction transaction ) throws IOException
    {
        GraphDatabaseAPI database = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( directory ).setConfig( configuration( false ) ).newGraphDatabase();
        try ( org.neo4j.graphdb.Transaction tx = database.beginTx() )
        {
            DependencyResolver resolver = database.getDependencyResolver();
            XaDataSourceManager xaDataSourceManager = resolver.resolveDependency( XaDataSourceManager.class );
            NeoStoreXaDataSource dataSource = xaDataSourceManager.getNeoStoreDataSource();
            dataSource.applyPreparedTransaction( write( transaction, dataSource.getLastCommittedTxId() ) );
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
    private int localIdGenerator = 0;
    private long schemaId;
    private long nodeId;
    private int labelId;
    private long nodeLabelsId;
    private long relId;
    private int propId;
    private long stringPropId;
    private long arrayPropId;
    private int relTypeId;
    private int propKeyId;

    private void generateInitialData()
    {
        GraphDatabaseAPI graphDb = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( directory ).setConfig( configuration( true ) ).newGraphDatabase();
        try
        {
            generateInitialData( graphDb );
            StoreAccess stores = new StoreAccess( graphDb );
            schemaId = stores.getSchemaStore().getHighId();
            nodeId = stores.getNodeStore().getHighId();
            labelId = (int) stores.getLabelTokenStore().getHighId();
            nodeLabelsId = stores.getNodeDynamicLabelStore().getHighId();
            relId = stores.getRelationshipStore().getHighId();
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
        return directory.apply( new Statement()
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
        }, description );
    }
}
