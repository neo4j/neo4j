/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.IdType;

public class TestUpgradeStore
{
    private static final String PATH = "target/var/upgrade";
    
    @Before
    public void doBefore()
    {
        deleteFileOrDirectory( PATH );
    }
    
    @Test
    public void makeSureStoreWithTooManyRelationshipTypesCannotBeUpgraded() throws Exception
    {
        new EmbeddedGraphDatabase( PATH ).shutdown();
        createManyRelationshipTypes( 0xFFFF+10 );
        assertCannotStart( "Shouldn't be able to upgrade with that many types set" );
    }
    
    @Test
    public void makeSureStoreWithDecentAmountOfRelationshipTypesCanBeUpgraded() throws Exception
    {
        new EmbeddedGraphDatabase( PATH ).shutdown();
        createManyRelationshipTypes( 0xFFFF-10 );
        assertCanStart();
    }
    
    @Test( expected=TransactionFailureException.class )
    public void makeSureStoreWithTooBigStringBlockSizeCannotBeCreated() throws Exception
    {
        new EmbeddedGraphDatabase( PATH, stringMap( STRING_BLOCK_SIZE, "" + (0x10000) ) );
    }
    
    @Test
    public void makeSureStoreWithDecentStringBlockSizeCanBeCreated() throws Exception
    {
        new EmbeddedGraphDatabase( PATH, stringMap( STRING_BLOCK_SIZE, "" + (0xFFFF) ) ).shutdown();
    }
    
    @Test( expected=TransactionFailureException.class )
    public void makeSureStoreWithTooBigArrayBlockSizeCannotBeCreated() throws Exception
    {
        new EmbeddedGraphDatabase( PATH, stringMap( ARRAY_BLOCK_SIZE, "" + (0x10000) ) );
    }
    
    @Test
    public void makeSureStoreWithDecentArrayBlockSizeCanBeCreated() throws Exception
    {
        new EmbeddedGraphDatabase( PATH, stringMap( ARRAY_BLOCK_SIZE, "" + (0xFFFF) ) ).shutdown();
    }
    
    @Test
    public void makeSureStoreWithTooBigStringBlockSizeCannotBeUpgraded() throws Exception
    {
        new EmbeddedGraphDatabase( PATH ).shutdown();
        setBlockSize( new File( PATH, "neostore.propertystore.db.strings" ), 0x10000, "StringPropertyStore v0.9.5" );
        assertCannotStart( "Shouldn't be able to upgrade with block size that big" );
    }
    
    @Test
    public void makeSureStoreWithDecentStringBlockSizeCanBeUpgraded() throws Exception
    {
        new EmbeddedGraphDatabase( PATH ).shutdown();
        setBlockSize( new File( PATH, "neostore.propertystore.db.strings" ), 0xFFFF, "StringPropertyStore v0.9.5" );
        assertCanStart();
    }
    
    @Test
    public void makeSureStoreWithTooBigArrayBlockSizeCannotBeUpgraded() throws Exception
    {
        new EmbeddedGraphDatabase( PATH ).shutdown();
        setBlockSize( new File( PATH, "neostore.propertystore.db.arrays" ), 0x10000, "ArrayPropertyStore v0.9.5" );
        assertCannotStart( "Shouldn't be able to upgrade with block size that big" );
    }
    
    @Test
    public void makeSureStoreWithDecentArrayBlockSizeCanBeUpgraded() throws Exception
    {
        new EmbeddedGraphDatabase( PATH ).shutdown();
        setBlockSize( new File( PATH, "neostore.propertystore.db.arrays" ), 0xFFFF, "ArrayPropertyStore v0.9.5" );
        assertCanStart();
    }
    
    @Test
    public void makeSureLogsAreMovedWhenUpgrading() throws Exception
    {
        // Generate some logical logs
        for ( int i = 0; i < 3; i++ )
        {
            new EmbeddedGraphDatabase( PATH, stringMap( KEEP_LOGICAL_LOGS, "true" ) ).shutdown();
        }
        
        setOlderNeoStoreVersion();
        new EmbeddedGraphDatabase( PATH, stringMap( ALLOW_STORE_UPGRADE, "true" ) ).shutdown();
        
        File oldLogDir = new File( PATH, "1.2-logs" );
        assertTrue( oldLogDir.exists() );
        assertTrue( new File( oldLogDir, "nioneo_logical.log.v0" ).exists() );
        assertTrue( new File( oldLogDir, "nioneo_logical.log.v1" ).exists() );
        assertTrue( new File( oldLogDir, "nioneo_logical.log.v2" ).exists() );
        assertFalse( new File( PATH, "nioneo_logical.log.v0" ).exists() );
        assertFalse( new File( PATH, "nioneo_logical.log.v1" ).exists() );
        assertFalse( new File( PATH, "nioneo_logical.log.v2" ).exists() );
    }
    
    private void assertCannotStart( String failMessage )
    {
        GraphDatabaseService db = null;
        try
        {
            db = new EmbeddedGraphDatabase( PATH );
            fail( failMessage );
        }
        catch ( TransactionFailureException e )
        {
            if ( !( e.getCause() instanceof IllegalStoreVersionException ) )
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

    private void assertCanStart()
    {
        GraphDatabaseService db = null;
        try
        {
            db = new EmbeddedGraphDatabase( PATH );
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }
    
    private void setOlderNeoStoreVersion() throws IOException
    {
        String oldVersion = "NeoStore v0.9.6";
        FileChannel channel = new RandomAccessFile( new File( PATH, "neostore" ), "rw" ).getChannel();
        channel.position( channel.size()-oldVersion.getBytes().length );
        ByteBuffer buffer = ByteBuffer.wrap( oldVersion.getBytes() );
        channel.write( buffer );
        channel.close();
    }

    private void setBlockSize( File file, int blockSize, String oldVersionToSet ) throws IOException
    {
        FileChannel channel = new RandomAccessFile( file, "rw" ).getChannel();
        ByteBuffer buffer = ByteBuffer.wrap( new byte[4] );
        // This +13 thing is done internally when creating the store
        // since a block has an overhead of 13 bytes
        buffer.putInt( blockSize+13 );
        buffer.flip();
        channel.write( buffer );
        
        // It's the same length as the current version v0.9.9
        channel.position( channel.size()-oldVersionToSet.getBytes().length );
        buffer = ByteBuffer.wrap( oldVersionToSet.getBytes() );
        channel.write( buffer );
        channel.close();
    }

    private void createManyRelationshipTypes( int numberOfTypes )
    {
        String fileName = new File( PATH, "neostore.relationshiptypestore.db" ).getAbsolutePath();
        Map<Object, Object> config = MapUtil.<Object, Object>genericMap( IdGeneratorFactory.class, new NoLimitidGeneratorFactory() );
        RelationshipTypeStore store = new RelationshipTypeStoreWithOneOlderVersion( fileName, config, IdType.RELATIONSHIP_TYPE );
        for ( int i = 0; i < numberOfTypes; i++ )
        {
            String name = "type" + i;
            RelationshipTypeRecord record = new RelationshipTypeRecord( i );
            record.setCreated();
            record.setInUse( true );
            int typeBlockId = (int) store.nextBlockId();
            record.setTypeBlock( typeBlockId );
            int length = name.length();
            char[] chars = new char[length];
            name.getChars( 0, length, chars, 0 );
            Collection<DynamicRecord> typeRecords = store.allocateTypeNameRecords( typeBlockId, chars );
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
        public String getTypeAndVersionDescriptor()
        {
            // This funky method will trick the store, telling it that it's the new version
            // when it loads (so that it validates OK). Then when closing it and writing
            // the version it will write the older version.
            if ( !versionCalled )
            {
                versionCalled = true;
                return super.getTypeAndVersionDescriptor();
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
                long highestIdInUse )
        {
            IdGenerator generator = new IdGeneratorImpl( fileName, grabSize, Long.MAX_VALUE );
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
