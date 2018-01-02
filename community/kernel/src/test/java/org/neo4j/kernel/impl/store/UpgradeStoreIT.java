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
package org.neo4j.kernel.impl.store;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.UTF8;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogFile;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.PageCacheRule;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.AbstractNeo4jTestCase.deleteFileOrDirectory;

@Ignore
public class UpgradeStoreIT
{
    private static final String PATH = "target/var/upgrade";

    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    @Before
    public void doBefore()
    {
        deleteFileOrDirectory( PATH );
    }

    private File path( int i )
    {
        return new File( PATH, "" + i ).getAbsoluteFile();
    }

    @Test
    public void makeSureStoreWithTooManyRelationshipTypesCannotBeUpgraded() throws Exception
    {
        File path = path( 0 );
        startAndStopDb( path );
        createManyRelationshipTypes( path, 0x10000 );
        assertCannotStart( path, "Shouldn't be able to upgrade with that many types set" );
    }

    @Test
    public void makeSureStoreWithDecentAmountOfRelationshipTypesCanBeUpgraded() throws Exception
    {
        File path = path( 1 );
        startAndStopDb( path );
        createManyRelationshipTypes( path, 0xFFFF );
        assertCanStart( path );
    }

    @Test( expected=TransactionFailureException.class )
    public void makeSureStoreWithTooBigStringBlockSizeCannotBeCreated() throws Exception
    {
        builderFor( path( 2 ) ).setConfig(
                GraphDatabaseSettings.string_block_size, "" + (0x10000) ).newGraphDatabase().shutdown();
    }

    @Test
    public void makeSureStoreWithDecentStringBlockSizeCanBeCreated() throws Exception
    {
        builderFor( path( 3 ) ).setConfig(
                GraphDatabaseSettings.string_block_size, "" + (0xFFFF) ).newGraphDatabase().shutdown();
    }

    @Test( expected=TransactionFailureException.class )
    public void makeSureStoreWithTooBigArrayBlockSizeCannotBeCreated() throws Exception
    {
        builderFor( path( 4 ) ).setConfig(
                GraphDatabaseSettings.array_block_size, "" + (0x10000) ).newGraphDatabase().shutdown();
    }

    @Test
    public void makeSureStoreWithDecentArrayBlockSizeCanBeCreated() throws Exception
    {
        builderFor( path( 5 ) ).setConfig(
                GraphDatabaseSettings.array_block_size, "" + (0xFFFF) ).newGraphDatabase().shutdown();
    }

    @Test
    public void makeSureStoreWithTooBigStringBlockSizeCannotBeUpgraded() throws Exception
    {
        File path = path( 6 );
        startAndStopDb( path );
        setBlockSize( new File( path, "neostore.propertystore.db.strings" ), 0x10000, "StringPropertyStore v0.9.5" );
        assertCannotStart( path, "Shouldn't be able to upgrade with block size that big" );
    }

    @Test
    public void makeSureStoreWithDecentStringBlockSizeCanBeUpgraded() throws Exception
    {
        File path = path( 7 );
        startAndStopDb( path );
        setBlockSize( new File( path, "neostore.propertystore.db.strings" ), 0xFFFF, "StringPropertyStore v0.9.5" );
        assertCanStart( path );
    }

    @Test
    public void makeSureStoreWithTooBigArrayBlockSizeCannotBeUpgraded() throws Exception
    {
        File path = path( 8 );
        startAndStopDb( path );
        setBlockSize( new File( path, "neostore.propertystore.db.arrays" ), 0x10000, "ArrayPropertyStore v0.9.5" );
        assertCannotStart( path, "Shouldn't be able to upgrade with block size that big" );
    }

    @Test
    public void makeSureStoreWithDecentArrayBlockSizeCanBeUpgraded() throws Exception
    {
        File path = path( 9 );
        startAndStopDb( path );
        setBlockSize( new File( path, "neostore.propertystore.db.arrays" ), 0xFFFF, "ArrayPropertyStore v0.9.5" );
        assertCanStart( path );
    }

    @Test
    public void makeSureLogsAreMovedWhenUpgrading() throws Exception
    {
        // Generate some logical logs
        File path = path( 10 );
        for ( int i = 0; i < 3; i++ )
        {
            builderFor( path ).setConfig( GraphDatabaseSettings.keep_logical_logs, Settings.TRUE ).newGraphDatabase().shutdown();
        }

        setOlderNeoStoreVersion( path );
        builderFor( path ).setConfig( GraphDatabaseSettings.allow_store_upgrade, Settings.TRUE ).newGraphDatabase().shutdown();

        File oldLogDir = new File( path, "1.2-logs" );
        assertTrue( oldLogDir.exists() );
        assertTrue( new File( oldLogDir, PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + "0" ).exists() );
        assertTrue( new File( oldLogDir, PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + "1" ).exists() );
        assertTrue( new File( oldLogDir, PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + "2" ).exists() );
        assertFalse( new File( path, PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + "0" ).exists() );
        assertFalse( new File( path, PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + "1" ).exists() );
        assertFalse( new File( path, PhysicalLogFile.DEFAULT_NAME + PhysicalLogFile.DEFAULT_VERSION_SUFFIX + "2" ).exists() );
    }

    @Test
    public void makeSureStoreCantBeUpgradedIfNotExplicitlyToldTo() throws Exception
    {
        File path = path( 11 );
        startAndStopDb( path );
        setOlderNeoStoreVersion( path );

        try
        {
            startDb( path );
            fail( "Shouldn't be able to upgrade if not told to" );
        }
        catch ( TransactionFailureException e )
        {
            if ( !( e.getCause() instanceof NotCurrentStoreVersionException) )
            {
                throw e;
            }
        }
    }

    @Test
    public void makeSureStoreCantBeUpgradedIfNotExplicitlyToldTo2() throws Exception
    {
        File path = path( 12 );
        startAndStopDb( path );
        setOlderNeoStoreVersion( path );

        try
        {
            builderFor( path ).setConfig(
                    GraphDatabaseSettings.allow_store_upgrade, Settings.TRUE ).newGraphDatabase().shutdown();
            fail( "Shouldn't be able to upgrade if not told to" );
        }
        catch ( TransactionFailureException e )
        {
            if ( !( e.getCause() instanceof NotCurrentStoreVersionException) )
            {
                throw e;
            }
        }
    }

    @Test
    public void makeSureStoreCanBeUpgradedIfExplicitlyToldTo() throws Exception
    {
        File path = path( 13 );
        startAndStopDb( path );
        setOlderNeoStoreVersion( path );
        builderFor( path ).setConfig( GraphDatabaseSettings.allow_store_upgrade, Settings.TRUE ).newGraphDatabase()
                          .shutdown();
    }

    @Test
    public void makeSureStoreCantBeUpgradedByBatchInserterEvenIfExplicitlyToldTo() throws Exception
    {
        File path = path( 14 );
        startAndStopDb( path );
        setOlderNeoStoreVersion( path );

        try
        {
            BatchInserters.inserter( path, stringMap( GraphDatabaseSettings.allow_store_upgrade.name(), Settings.TRUE ) );
            fail( "Shouldn't be able to upgrade with batch inserter" );
        }
        catch ( IllegalArgumentException e )
        {   // Good
        }
    }

    private void assertCannotStart( File path, String failMessage )
    {
        GraphDatabaseService db = null;
        try
        {
            db = startDb( path );
            fail( failMessage );
        }
        catch ( TransactionFailureException e )
        {
            if ( !( e.getCause() instanceof NotCurrentStoreVersionException) )
            {
                throw e;
            }
            // Good
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    private void assertCanStart( File path )
    {
        GraphDatabaseService db = null;
        try
        {
            db = startDb( path );
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    private GraphDatabaseService startDb( File path )
    {
        return new TestGraphDatabaseFactory().newEmbeddedDatabase( path.getAbsolutePath() );
    }

    private void startAndStopDb( File path )
    {
        startDb( path ).shutdown();
    }

    private GraphDatabaseBuilder builderFor( File path )
    {
        return new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( path.getAbsolutePath() );
    }

    private void setOlderNeoStoreVersion( File path ) throws IOException
    {
        String oldVersion = "NeoStore v0.9.6";
        FileChannel channel = new RandomAccessFile( new File( path, MetaDataStore.DEFAULT_NAME ), "rw" ).getChannel();
        channel.position( channel.size() - UTF8.encode( oldVersion ).length );
        ByteBuffer buffer = ByteBuffer.wrap( UTF8.encode( oldVersion ) );
        channel.write( buffer );
        channel.close();
    }

    private void setBlockSize( File file, int blockSize, String oldVersionToSet ) throws IOException
    {
        FileChannel channel = new RandomAccessFile( file, "rw" ).getChannel();
        ByteBuffer buffer = ByteBuffer.wrap( new byte[4] );
        buffer.putInt( blockSize + AbstractDynamicStore.BLOCK_HEADER_SIZE );
        buffer.flip();
        channel.write( buffer );

        // It's the same length as the current version
        channel.position( channel.size() - UTF8.encode( oldVersionToSet ).length );
        buffer = ByteBuffer.wrap( UTF8.encode( oldVersionToSet ) );
        channel.write( buffer );
        channel.close();
    }

    private void createManyRelationshipTypes( File path, int numberOfTypes )
    {
        File fileName = new File( path, "neostore.relationshiptypestore.db" );
        Config config = new Config();
        DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        DynamicStringStore stringStore = new DynamicStringStore( new File( fileName.getPath() + ".names" ), config,
                IdType.RELATIONSHIP_TYPE_TOKEN_NAME, new DefaultIdGeneratorFactory( fs ), pageCache,
                NullLogProvider.getInstance(), TokenStore.NAME_STORE_BLOCK_SIZE );
        RelationshipTypeTokenStore store =
                new RelationshipTypeTokenStoreWithOneOlderVersion( fileName, stringStore, fs, pageCache );
        for ( int i = 0; i < numberOfTypes; i++ )
        {
            String name = "type" + i;
            RelationshipTypeTokenRecord record = new RelationshipTypeTokenRecord( i );
            record.setCreated();
            record.setInUse( true );
            Collection<DynamicRecord> typeRecords = store.allocateNameRecords( PropertyStore.encodeString( name ) );
            record.setNameId( (int) first( typeRecords ).getId() );
            record.addNameRecords( typeRecords );
            store.setHighId( store.getHighId()+1 );
            store.updateRecord( record );
        }
        store.close();
    }

    private static class RelationshipTypeTokenStoreWithOneOlderVersion extends RelationshipTypeTokenStore
    {
        private static final Config config = new Config( stringMap() );
        private boolean versionCalled;

        public RelationshipTypeTokenStoreWithOneOlderVersion(
                File fileName,
                DynamicStringStore stringStore,
                FileSystemAbstraction fs,
                PageCache pageCache )
        {
            super( fileName, config, new NoLimitIdGeneratorFactory( fs ), pageCache, NullLogProvider.getInstance(),
                    stringStore );
        }

        @Override
        public String getTypeDescriptor()
        {
            // This funky method will trick the store, telling it that it's the new version
            // when it loads (so that it validates OK). Then when closing it and writing
            // the version it will write the older version.
            if ( !versionCalled )
            {
                versionCalled = true;
                return super.getTypeDescriptor();
            }
            else
            {
                // TODO This shouldn't be hard coded like this, boring to keep in sync
                // when version changes
                return "RelationshipTypeStore v0.9.5";
            }
        }
    }

    private static class NoLimitIdGeneratorFactory implements IdGeneratorFactory
    {
        private final Map<IdType, IdGenerator> generators = new HashMap<>();
        private final FileSystemAbstraction fs;

        public NoLimitIdGeneratorFactory( FileSystemAbstraction fs )
        {
            this.fs = fs;
        }

        @Override
        public IdGenerator open( File filename, IdType idType, long highId )
        {
            return open( filename, 0, idType, highId );
        }

        @Override
        public IdGenerator open( File fileName, int grabSize, IdType idType, long highId )
        {
            IdGenerator generator = new IdGeneratorImpl( fs, fileName, grabSize, Long.MAX_VALUE, false, highId );
            generators.put( idType, generator );
            return generator;
        }

        @Override
        public IdGenerator get( IdType idType )
        {
            return generators.get( idType );
        }

        @Override
        public void create( File fileName, long highId, boolean throwIfFileExists )
        {
            IdGeneratorImpl.createGenerator( fs, fileName, highId, throwIfFileExists );
        }
    }
}
