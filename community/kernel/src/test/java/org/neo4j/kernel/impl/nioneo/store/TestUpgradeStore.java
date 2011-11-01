/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import static org.neo4j.kernel.Config.ALLOW_STORE_UPGRADE;
import static org.neo4j.kernel.Config.ARRAY_BLOCK_SIZE;
import static org.neo4j.kernel.Config.KEEP_LOGICAL_LOGS;
import static org.neo4j.kernel.Config.STRING_BLOCK_SIZE;
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
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;

@Ignore
public class TestUpgradeStore
{
    private static final String PATH = "target/var/upgrade";

    @Before
    public void doBefore()
    {
        deleteFileOrDirectory( PATH );
    }

    private String path( int i )
    {
        return new File( PATH, "" + i ).getAbsolutePath();
    }

    @Test
    public void makeSureStoreWithTooManyRelationshipTypesCannotBeUpgraded() throws Exception
    {
        String path = path( 0 );
        new EmbeddedGraphDatabase( path ).shutdown();
        createManyRelationshipTypes( path, 0x10000 );
        assertCannotStart( path, "Shouldn't be able to upgrade with that many types set" );
    }

    @Test
    public void makeSureStoreWithDecentAmountOfRelationshipTypesCanBeUpgraded() throws Exception
    {
        String path = path( 1 );
        new EmbeddedGraphDatabase( path ).shutdown();
        createManyRelationshipTypes( path, 0xFFFF );
        assertCanStart( path );
    }

    @Test( expected=TransactionFailureException.class )
    public void makeSureStoreWithTooBigStringBlockSizeCannotBeCreated() throws Exception
    {
        new EmbeddedGraphDatabase( path( 2 ), stringMap( STRING_BLOCK_SIZE, "" + (0x10000) ) );
    }

    @Test
    public void makeSureStoreWithDecentStringBlockSizeCanBeCreated() throws Exception
    {
        new EmbeddedGraphDatabase( path( 3 ), stringMap( STRING_BLOCK_SIZE, "" + (0xFFFF) ) ).shutdown();
    }

    @Test( expected=TransactionFailureException.class )
    public void makeSureStoreWithTooBigArrayBlockSizeCannotBeCreated() throws Exception
    {
        new EmbeddedGraphDatabase( path( 4 ), stringMap( ARRAY_BLOCK_SIZE, "" + (0x10000) ) );
    }

    @Test
    public void makeSureStoreWithDecentArrayBlockSizeCanBeCreated() throws Exception
    {
        new EmbeddedGraphDatabase( path( 5 ), stringMap( ARRAY_BLOCK_SIZE, "" + (0xFFFF) ) ).shutdown();
    }

    @Test
    public void makeSureStoreWithTooBigStringBlockSizeCannotBeUpgraded() throws Exception
    {
        String path = path( 6 );
        new EmbeddedGraphDatabase( path ).shutdown();
        setBlockSize( new File( path, "neostore.propertystore.db.strings" ), 0x10000, "StringPropertyStore v0.9.5" );
        assertCannotStart( path, "Shouldn't be able to upgrade with block size that big" );
    }

    @Test
    public void makeSureStoreWithDecentStringBlockSizeCanBeUpgraded() throws Exception
    {
        String path = path( 7 );
        new EmbeddedGraphDatabase( path ).shutdown();
        setBlockSize( new File( path, "neostore.propertystore.db.strings" ), 0xFFFF, "StringPropertyStore v0.9.5" );
        assertCanStart( path );
    }

    @Test
    public void makeSureStoreWithTooBigArrayBlockSizeCannotBeUpgraded() throws Exception
    {
        String path = path( 8 );
        new EmbeddedGraphDatabase( path ).shutdown();
        setBlockSize( new File( path, "neostore.propertystore.db.arrays" ), 0x10000, "ArrayPropertyStore v0.9.5" );
        assertCannotStart( path, "Shouldn't be able to upgrade with block size that big" );
    }

    @Test
    public void makeSureStoreWithDecentArrayBlockSizeCanBeUpgraded() throws Exception
    {
        String path = path( 9 );
        new EmbeddedGraphDatabase( path ).shutdown();
        setBlockSize( new File( path, "neostore.propertystore.db.arrays" ), 0xFFFF, "ArrayPropertyStore v0.9.5" );
        assertCanStart( path );
    }

    @Test
    public void makeSureLogsAreMovedWhenUpgrading() throws Exception
    {
        // Generate some logical logs
        String path = path( 10 );
        for ( int i = 0; i < 3; i++ )
        {
            new EmbeddedGraphDatabase( path, stringMap( KEEP_LOGICAL_LOGS, "true" ) ).shutdown();
        }

        setOlderNeoStoreVersion( path );
        new EmbeddedGraphDatabase( path, stringMap( ALLOW_STORE_UPGRADE, "true" ) ).shutdown();

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
        String path = path( 11 );
        new EmbeddedGraphDatabase( path ).shutdown();
        setOlderNeoStoreVersion( path );

        try
        {
            new EmbeddedGraphDatabase( path );
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
        String path = path( 12 );
        new EmbeddedGraphDatabase( path ).shutdown();
        setOlderNeoStoreVersion( path );

        try
        {
            new EmbeddedGraphDatabase( path, stringMap( ALLOW_STORE_UPGRADE, "false" ) );
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
        String path = path( 13 );
        new EmbeddedGraphDatabase( path ).shutdown();
        setOlderNeoStoreVersion( path );
        new EmbeddedGraphDatabase( path, stringMap( ALLOW_STORE_UPGRADE, "true" ) ).shutdown();
    }

    @Test
    public void makeSureStoreCantBeUpgradedByBatchInserterEvenIfExplicitlyToldTo() throws Exception
    {
        String path = path( 14 );
        new EmbeddedGraphDatabase( path ).shutdown();
        setOlderNeoStoreVersion( path );

        try
        {
            new BatchInserterImpl( path, stringMap( ALLOW_STORE_UPGRADE, "true" ) );
            fail( "Shouldn't be able to upgrade with batch inserter" );
        }
        catch ( IllegalArgumentException e )
        {   // Good
        }
    }

    private void assertCannotStart( String path, String failMessage )
    {
        GraphDatabaseService db = null;
        try
        {
            db = new EmbeddedGraphDatabase( path );
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

    private void assertCanStart( String path )
    {
        GraphDatabaseService db = null;
        try
        {
            db = new EmbeddedGraphDatabase( path );
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    private void setOlderNeoStoreVersion( String path ) throws IOException
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

    private void createManyRelationshipTypes( String path, int numberOfTypes )
    {
        String fileName = new File( path, "neostore.relationshiptypestore.db" ).getAbsolutePath();
        Map<Object, Object> config = MapUtil.<Object, Object>genericMap(
                IdGeneratorFactory.class, new NoLimitidGeneratorFactory(),
                FileSystemAbstraction.class, CommonFactories.defaultFileSystemAbstraction() );
        RelationshipTypeStore store = new RelationshipTypeStoreWithOneOlderVersion( fileName, config, IdType.RELATIONSHIP_TYPE );
        for ( int i = 0; i < numberOfTypes; i++ )
        {
            String name = "type" + i;
            RelationshipTypeRecord record = new RelationshipTypeRecord( i );
            record.setCreated();
            record.setInUse( true );
            int typeBlockId = (int) store.nextBlockId();
            record.setTypeBlock( typeBlockId );
            Collection<DynamicRecord> typeRecords = store.allocateTypeNameRecords( typeBlockId, PropertyStore.encodeString( name ) );
            for ( DynamicRecord typeRecord : typeRecords )
            {
                record.addTypeRecord( typeRecord );
            }
            store.setHighId( store.getHighId()+1 );
            store.updateRecord( record );
        }
        store.close();
    }

    private static class RelationshipTypeStoreWithOneOlderVersion extends RelationshipTypeStore
    {
        private boolean versionCalled;

        public RelationshipTypeStoreWithOneOlderVersion( String fileName, Map<?, ?> config,
                IdType idType )
        {
            super( fileName, config, idType );
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

    private static class NoLimitidGeneratorFactory implements IdGeneratorFactory
    {
        private final Map<IdType, IdGenerator> generators = new HashMap<IdType, IdGenerator>();

        public IdGenerator open( String fileName, int grabSize, IdType idType,
                long highestIdInUse, boolean startup )
        {
            IdGenerator generator = new IdGeneratorImpl( fileName, grabSize, Long.MAX_VALUE, false );
            generators.put( idType, generator );
            return generator;
        }

        public IdGenerator get( IdType idType )
        {
            return generators.get( idType );
        }

        public void create( String fileName )
        {
            IdGeneratorImpl.createGenerator( fileName );
        }

        public void updateIdGenerators( NeoStore neoStore )
        {
            neoStore.updateIdGenerators();
        }
    }
}
