/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.AbstractNeo4jTestCase.deleteFileOrDirectory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.kernel.impl.util.StringLogger;

@Ignore
public class UpgradeStoreIT
{
    private static final String PATH = "target/var/upgrade";

    @Before
    public void doBefore()
    {
        deleteFileOrDirectory( PATH );
    }

    private File path( int i )
    {
        return new File( PATH, "" + i );
    }

    @Test
    public void makeSureStoreWithTooManyRelationshipTypesCannotBeUpgraded() throws Exception
    {
        File path = path( 0 );
        new EmbeddedGraphDatabase( path.getPath() ).shutdown();
        createManyRelationshipTypes( path, 0x10000 );
        assertCannotStart( path, "Shouldn't be able to upgrade with that many types set" );
    }

    @Test
    public void makeSureStoreWithDecentAmountOfRelationshipTypesCanBeUpgraded() throws Exception
    {
        File path = path( 1 );
        new EmbeddedGraphDatabase( path.getPath() ).shutdown();
        createManyRelationshipTypes( path, 0xFFFF );
        assertCanStart( path );
    }

    @Test( expected=TransactionFailureException.class )
    public void makeSureStoreWithTooBigStringBlockSizeCannotBeCreated() throws Exception
    {
        new EmbeddedGraphDatabase( path( 2 ).getPath(), stringMap( Config.STRING_BLOCK_SIZE, "" + (0x10000) ) );
    }

    @Test
    public void makeSureStoreWithDecentStringBlockSizeCanBeCreated() throws Exception
    {
        new EmbeddedGraphDatabase( path( 3 ).getPath(), stringMap( Config.STRING_BLOCK_SIZE, "" + (0xFFFF) ) ).shutdown();
    }

    @Test( expected=TransactionFailureException.class )
    public void makeSureStoreWithTooBigArrayBlockSizeCannotBeCreated() throws Exception
    {
        new EmbeddedGraphDatabase( path( 4 ).getPath(), stringMap( Config.ARRAY_BLOCK_SIZE, "" + (0x10000) ) );
    }

    @Test
    public void makeSureStoreWithDecentArrayBlockSizeCanBeCreated() throws Exception
    {
        new EmbeddedGraphDatabase( path( 5 ).getPath(), stringMap( Config.ARRAY_BLOCK_SIZE, "" + (0xFFFF) ) ).shutdown();
    }

    @Test
    public void makeSureStoreWithTooBigStringBlockSizeCannotBeUpgraded() throws Exception
    {
        File path = path( 6 );
        new EmbeddedGraphDatabase( path.getPath() ).shutdown();
        setBlockSize( new File( path, "neostore.propertystore.db.strings" ), 0x10000, "StringPropertyStore v0.9.5" );
        assertCannotStart( path, "Shouldn't be able to upgrade with block size that big" );
    }

    @Test
    public void makeSureStoreWithDecentStringBlockSizeCanBeUpgraded() throws Exception
    {
        File path = path( 7 );
        new EmbeddedGraphDatabase( path.getPath() ).shutdown();
        setBlockSize( new File( path, "neostore.propertystore.db.strings" ), 0xFFFF, "StringPropertyStore v0.9.5" );
        assertCanStart( path );
    }

    @Test
    public void makeSureStoreWithTooBigArrayBlockSizeCannotBeUpgraded() throws Exception
    {
        File path = path( 8 );
        new EmbeddedGraphDatabase( path.getPath() ).shutdown();
        setBlockSize( new File( path, "neostore.propertystore.db.arrays" ), 0x10000, "ArrayPropertyStore v0.9.5" );
        assertCannotStart( path, "Shouldn't be able to upgrade with block size that big" );
    }

    @Test
    public void makeSureStoreWithDecentArrayBlockSizeCanBeUpgraded() throws Exception
    {
        File path = path( 9 );
        new EmbeddedGraphDatabase( path.getPath() ).shutdown();
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
            new EmbeddedGraphDatabase( path.getPath(), stringMap( GraphDatabaseSettings.keep_logical_logs.name(), Settings.TRUE ) ).shutdown();
        }

        setOlderNeoStoreVersion( path );
        new EmbeddedGraphDatabase( path.getPath(), stringMap( GraphDatabaseSettings.allow_store_upgrade.name(), Settings.TRUE ) ).shutdown();

        File oldLogDir = new File( path, "1.2-logs" );
        assertTrue( oldLogDir.exists() );
        assertTrue( new File( oldLogDir, "nioneo_logical.log.v0" ).exists() );
        assertTrue( new File( oldLogDir, "nioneo_logical.log.v1" ).exists() );
        assertTrue( new File( oldLogDir, "nioneo_logical.log.v2" ).exists() );
        assertFalse( new File( path, "nioneo_logical.log.v0" ).exists() );
        assertFalse( new File( path, "nioneo_logical.log.v1" ).exists() );
        assertFalse( new File( path, "nioneo_logical.log.v2" ).exists() );
    }

    @Test
    public void makeSureStoreCantBeUpgradedIfNotExplicitlyToldTo() throws Exception
    {
        File path = path( 11 );
        new EmbeddedGraphDatabase( path.getPath() ).shutdown();
        setOlderNeoStoreVersion( path );

        try
        {
            new EmbeddedGraphDatabase( path.getPath() );
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
        new EmbeddedGraphDatabase( path.getPath() ).shutdown();
        setOlderNeoStoreVersion( path );

        try
        {
            new EmbeddedGraphDatabase( path.getPath(), stringMap( GraphDatabaseSettings.allow_store_upgrade.name(), Settings.TRUE ) );
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
        new EmbeddedGraphDatabase( path.getPath() ).shutdown();
        setOlderNeoStoreVersion( path );
        new EmbeddedGraphDatabase( path.getPath(), stringMap( GraphDatabaseSettings.allow_store_upgrade.name(), Settings.TRUE ) ).shutdown();
    }

    @Test
    public void makeSureStoreCantBeUpgradedByBatchInserterEvenIfExplicitlyToldTo() throws Exception
    {
        File path = path( 14 );
        new EmbeddedGraphDatabase( path.getPath() ).shutdown();
        setOlderNeoStoreVersion( path );

        try
        {
            new BatchInserterImpl( path.getPath(), stringMap( GraphDatabaseSettings.allow_store_upgrade.name(), Settings.TRUE ) );
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
            db = new EmbeddedGraphDatabase( path.getPath() );
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
            db = new EmbeddedGraphDatabase( path.getPath() );
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    private void setOlderNeoStoreVersion( File path ) throws IOException
    {
        String oldVersion = "NeoStore v0.9.6";
        FileChannel channel = new RandomAccessFile( new File( path, NeoStore.DEFAULT_NAME ), "rw" ).getChannel();
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

        // It's the same length as the current version v0.9.9
        channel.position( channel.size() - UTF8.encode( oldVersionToSet ).length );
        buffer = ByteBuffer.wrap( UTF8.encode( oldVersionToSet ) );
        channel.write( buffer );
        channel.close();
    }

    private void createManyRelationshipTypes( File path, int numberOfTypes )
    {
        File fileName = new File( path, "neostore.relationshiptypestore.db" );
        DynamicStringStore stringStore = new DynamicStringStore( new File( fileName.getPath() + ".names"), null, IdType.RELATIONSHIP_TYPE_BLOCK,
                new DefaultIdGeneratorFactory(), new DefaultWindowPoolFactory(), new DefaultFileSystemAbstraction(), StringLogger.SYSTEM );
        RelationshipTypeStore store = new RelationshipTypeStoreWithOneOlderVersion( fileName, stringStore );
        for ( int i = 0; i < numberOfTypes; i++ )
        {
            String name = "type" + i;
            RelationshipTypeRecord record = new RelationshipTypeRecord( i );
            record.setCreated();
            record.setInUse( true );
            int nameId = (int) store.nextNameId();
            record.setNameId( nameId );
            Collection<DynamicRecord> typeRecords = store.allocateNameRecords( nameId, PropertyStore.encodeString( name ) );
            for ( DynamicRecord typeRecord : typeRecords )
            {
                record.addNameRecord( typeRecord );
            }
            store.setHighId( store.getHighId()+1 );
            store.updateRecord( record );
        }
        store.close();
    }

    private static class RelationshipTypeStoreWithOneOlderVersion extends RelationshipTypeStore
    {
        private boolean versionCalled;

        public RelationshipTypeStoreWithOneOlderVersion( File fileName, DynamicStringStore stringStore )
        {
            super( fileName, new Config( stringMap() ), new NoLimitIdGeneratorFactory(), new DefaultWindowPoolFactory(),
                    new DefaultFileSystemAbstraction(), StringLogger.SYSTEM, stringStore );
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
        private final Map<IdType, IdGenerator> generators = new HashMap<IdType, IdGenerator>();

        public IdGenerator open( FileSystemAbstraction fs, File fileName, int grabSize, IdType idType, long highId )
        {
            IdGenerator generator = new IdGeneratorImpl( fs, fileName, grabSize, Long.MAX_VALUE, false, highId );
            generators.put( idType, generator );
            return generator;
        }

        public IdGenerator get( IdType idType )
        {
            return generators.get( idType );
        }

        public void create( FileSystemAbstraction fs, File fileName, long highId )
        {
            IdGeneratorImpl.createGenerator( fs, fileName, highId );
        }
    }
}
