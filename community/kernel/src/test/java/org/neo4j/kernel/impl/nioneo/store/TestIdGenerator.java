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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.IteratorUtil.lastOrNull;
import static org.neo4j.kernel.impl.util.FileUtils.deleteRecursively;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

public class TestIdGenerator
{
    @Before
    public void deleteIdGeneratorFile()
    {
        new File( idGeneratorFile() ).delete();
    }
    
    private String path()
    {
        String path = AbstractNeo4jTestCase.getStorePath( "xatest" );
        new File( path ).mkdirs();
        return path;
    }
    
    private String file( String name )
    {
        return path() + File.separator + name;
    }
    
    private String idGeneratorFile()
    {
        return file( "testIdGenerator.id" );
    }
    
    @Test
    public void testCreateIdGenerator() throws IOException
    {
        try
        {
            IdGeneratorImpl.createGenerator( null );
            fail( "Null filename should throw exception" );
        }
        catch ( IllegalArgumentException e )
        {
        } // good
        try
        {
            IdGeneratorImpl.createGenerator( idGeneratorFile() );
            new IdGeneratorImpl( idGeneratorFile(), 0, 100 ).close();
            fail( "Zero grab size should throw exception" );
        }
        catch ( IllegalArgumentException e )
        {
        } // good
        try
        {
            new IdGeneratorImpl( "testIdGenerator.id", -1, 100 ).close();
            fail( "Negative grab size should throw exception" );
        }
        catch ( IllegalArgumentException e )
        {
        } // good

        try
        {
            IdGenerator idGenerator = new IdGeneratorImpl( idGeneratorFile(),
                1008, 1000 );
            try
            {
                IdGeneratorImpl.createGenerator( idGeneratorFile() );
                fail( "Creating a id generator with existing file name "
                    + "should throw exception" );
            }
            catch ( IllegalStateException e )
            {
            } // good
            idGenerator.close();
            // verify that id generator is ok
            FileChannel fileChannel = new FileInputStream( 
                idGeneratorFile() ).getChannel();
            ByteBuffer buffer = ByteBuffer.allocate( 9 );
            assertEquals( 9, fileChannel.read( buffer ) );
            buffer.flip();
            assertEquals( (byte) 0, buffer.get() );
            assertEquals( 0l, buffer.getLong() );
            buffer.flip();
            int readCount = fileChannel.read( buffer );
            if ( readCount != -1 && readCount != 0 )
            {
                fail( "Id generator header not ok read 9 + " + readCount
                    + " bytes from file" );
            }
            fileChannel.close();
        }
        finally
        {
            File file = new File( idGeneratorFile() );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    @Test
    public void testStickyGenerator()
    {
        try
        {
            IdGeneratorImpl.createGenerator( idGeneratorFile() );
            IdGenerator idGen = new IdGeneratorImpl( idGeneratorFile(), 3, 1000 );
            try
            {
                new IdGeneratorImpl( idGeneratorFile(), 3, 1000 );
                fail( "Opening sticky id generator should throw exception" );
            }
            catch ( StoreFailureException e )
            { // good
            }
            idGen.close();
        }
        finally
        {
            File file = new File( idGeneratorFile() );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    @Test
    public void testNextId()
    {
        try
        {
            IdGeneratorImpl.createGenerator( idGeneratorFile() );
            IdGenerator idGenerator = new IdGeneratorImpl( idGeneratorFile(), 3, 1000 );
            for ( long i = 0; i < 7; i++ )
            {
                assertEquals( i, idGenerator.nextId() );
            }
            idGenerator.freeId( 1 );
            idGenerator.freeId( 3 );
            idGenerator.freeId( 5 );
            assertEquals( 7l, idGenerator.nextId() );
            idGenerator.freeId( 6 );
            idGenerator.close();
            idGenerator = new IdGeneratorImpl( idGeneratorFile(), 5, 1000 );
            idGenerator.freeId( 2 );
            idGenerator.freeId( 4 );
            assertEquals( 1l, idGenerator.nextId() );
            idGenerator.freeId( 1 );
            assertEquals( 3l, idGenerator.nextId() );
            idGenerator.freeId( 3 );
            assertEquals( 5l, idGenerator.nextId() );
            idGenerator.freeId( 5 );
            assertEquals( 6l, idGenerator.nextId() );
            idGenerator.freeId( 6 );
            assertEquals( 8l, idGenerator.nextId() );
            idGenerator.freeId( 8 );
            assertEquals( 9l, idGenerator.nextId() );
            idGenerator.freeId( 9 );
            idGenerator.close();
            idGenerator = new IdGeneratorImpl( idGeneratorFile(), 3, 1000 );
            assertEquals( 2l, idGenerator.nextId() );
            assertEquals( 4l, idGenerator.nextId() );
            assertEquals( 1l, idGenerator.nextId() );
            assertEquals( 3l, idGenerator.nextId() );
            assertEquals( 5l, idGenerator.nextId() );
            assertEquals( 6l, idGenerator.nextId() );
            assertEquals( 8l, idGenerator.nextId() );
            assertEquals( 9l, idGenerator.nextId() );
            assertEquals( 10l, idGenerator.nextId() );
            assertEquals( 11l, idGenerator.nextId() );
            idGenerator.close();
        }
        finally
        {
            File file = new File( idGeneratorFile() );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    @Test
    public void testFreeId()
    {
        try
        {
            IdGeneratorImpl.createGenerator( idGeneratorFile() );
            IdGenerator idGenerator = new IdGeneratorImpl( idGeneratorFile(), 3, 1000 );
            for ( long i = 0; i < 7; i++ )
            {
                assertEquals( i, idGenerator.nextId() );
            }
            try
            {
                idGenerator.freeId( -1 );
                fail( "Negative id should throw exception" );
            }
            catch ( IllegalArgumentException e )
            { // good
            }
            try
            {
                idGenerator.freeId( 7 );
                fail( "Greater id than ever returned should throw exception" );
            }
            catch ( IllegalArgumentException e )
            { // good
            }
            for ( int i = 0; i < 7; i++ )
            {
                idGenerator.freeId( i );
            }
            idGenerator.close();
            idGenerator = new IdGeneratorImpl( idGeneratorFile(), 2, 1000 );
            assertEquals( 0l, idGenerator.nextId() );
            assertEquals( 1l, idGenerator.nextId() );
            assertEquals( 2l, idGenerator.nextId() );
            idGenerator.close();
            idGenerator = new IdGeneratorImpl( idGeneratorFile(), 2, 1000 );
            assertEquals( 4l, idGenerator.nextId() );
            assertEquals( 5l, idGenerator.nextId() );
            assertEquals( 6l, idGenerator.nextId() );
            assertEquals( 3l, idGenerator.nextId() );
            idGenerator.close();
        }
        finally
        {
            File file = new File( idGeneratorFile() );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    @Test
    public void testClose()
    {
        try
        {
            IdGeneratorImpl.createGenerator( idGeneratorFile() );
            IdGenerator idGenerator = new IdGeneratorImpl( idGeneratorFile(), 2, 1000 );
            idGenerator.close();
            try
            {
                idGenerator.nextId();
                fail( "nextId after close should throw exception" );
            }
            catch ( IllegalStateException e )
            { // good
            }
            try
            {
                idGenerator.freeId( 0 );
                fail( "freeId after close should throw exception" );
            }
            catch ( IllegalStateException e )
            { // good
            }
            idGenerator = new IdGeneratorImpl( idGeneratorFile(), 2, 1000 );
            assertEquals( 0l, idGenerator.nextId() );
            assertEquals( 1l, idGenerator.nextId() );
            assertEquals( 2l, idGenerator.nextId() );
            idGenerator.close();
            try
            {
                idGenerator.nextId();
                fail( "nextId after close should throw exception" );
            }
            catch ( IllegalStateException e )
            { // good
            }
            try
            {
                idGenerator.freeId( 0 );
                fail( "freeId after close should throw exception" );
            }
            catch ( IllegalStateException e )
            { // good
            }
        }
        finally
        {
            File file = new File( idGeneratorFile() );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    @Test
    public void testOddAndEvenWorstCase()
    {
        int capacity = 1024 * 8 + 1;
        try
        {
            IdGeneratorImpl.createGenerator( idGeneratorFile() );
            IdGenerator idGenerator = new IdGeneratorImpl( idGeneratorFile(),
                128, capacity*2 );
            for ( int i = 0; i < capacity; i++ )
            {
                idGenerator.nextId();
            }
            Map<Long,Object> freedIds = new HashMap<Long,Object>();
            for ( long i = 1; i < capacity; i += 2 )
            {
                idGenerator.freeId( i );
                freedIds.put( i, this );
            }
            idGenerator.close();
            idGenerator = new IdGeneratorImpl( idGeneratorFile(), 2000, capacity*2 );
            long oldId = -1;
            for ( int i = 0; i < capacity - 1; i += 2 )
            {
                long id = idGenerator.nextId();
                if ( freedIds.remove( id ) == null )
                {
                    throw new RuntimeException( "Id=" + id + " prevId=" + oldId
                        + " list.size()=" + freedIds.size() );
                }
                oldId = id;
            }
            assertTrue( freedIds.values().size() == 0 );
            idGenerator.close();
        }
        finally
        {
            File file = new File( idGeneratorFile() );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
        try
        {
            IdGeneratorImpl.createGenerator( idGeneratorFile() );
            IdGenerator idGenerator = new IdGeneratorImpl( idGeneratorFile(),
                128, capacity*2 );
            for ( int i = 0; i < capacity; i++ )
            {
                idGenerator.nextId();
            }
            Map<Long,Object> freedIds = new HashMap<Long,Object>();
            for ( long i = 0; i < capacity; i += 2 )
            {
                idGenerator.freeId( i );
                freedIds.put( i, this );
            }
            idGenerator.close();
            idGenerator = new IdGeneratorImpl( idGeneratorFile(), 2000, capacity*2 );
            for ( int i = 0; i < capacity; i += 2 )
            {
                assertEquals( this, freedIds.remove( idGenerator.nextId() ) );
            }
            assertEquals( 0, freedIds.values().size() );
            idGenerator.close();
        }
        finally
        {
            File file = new File( idGeneratorFile() );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    @Test
    public void testRandomTest()
    {
        int numberOfCloses = 0;
        java.util.Random random = new java.util.Random( System
            .currentTimeMillis() );
        int capacity = random.nextInt( 1024 ) + 1024;
        int grabSize = random.nextInt( 128 ) + 128;
        IdGeneratorImpl.createGenerator( idGeneratorFile() );
        IdGenerator idGenerator = new IdGeneratorImpl( idGeneratorFile(),
            grabSize, capacity*2 );
        List<Long> idsTaken = new ArrayList<Long>();
        float releaseIndex = 0.25f;
        float closeIndex = 0.05f;
        int currentIdCount = 0;
        try
        {
            while ( currentIdCount < capacity )
            {
                float rIndex = random.nextFloat();
                if ( rIndex < releaseIndex && currentIdCount > 0 )
                {
                    idGenerator.freeId( idsTaken.remove(
                        random.nextInt( currentIdCount ) ).intValue() );
                    currentIdCount--;
                }
                else
                {
                    idsTaken.add( idGenerator.nextId() );
                    currentIdCount++;
                }
                if ( rIndex > (1.0f - closeIndex) || rIndex < closeIndex )
                {
                    idGenerator.close();
                    grabSize = random.nextInt( 128 ) + 128;
                    idGenerator = new IdGeneratorImpl( idGeneratorFile(),
                        grabSize, capacity*2 );
                    numberOfCloses++;
                }
            }
            idGenerator.close();
        }
        finally
        {
            File file = new File( idGeneratorFile() );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }

    }

    @Test
    public void testUnsignedId()
    {
        try
        {
            IdGeneratorImpl.createGenerator( idGeneratorFile() );
            IdGenerator idGenerator = new IdGeneratorImpl( idGeneratorFile(), 1,
                    IdType.PROPERTY_INDEX.getMaxValue() );
            idGenerator.setHighId( IdType.PROPERTY_INDEX.getMaxValue()-1 );
            long id = idGenerator.nextId();
            assertEquals( IdType.PROPERTY_INDEX.getMaxValue()-1, id );
            idGenerator.freeId( id );
            try
            {
                idGenerator.nextId();
                fail( "Shouldn't be able to get next ID" );
            }
            catch ( StoreFailureException e ) 
            { // good, capacity exceeded
            }
            idGenerator.close();
            idGenerator = new IdGeneratorImpl( idGeneratorFile(), 1, IdType.PROPERTY_INDEX.getMaxValue() );
            assertEquals( IdType.PROPERTY_INDEX.getMaxValue()+1, idGenerator.getHighId() );
            id = idGenerator.nextId();
            assertEquals( IdType.PROPERTY_INDEX.getMaxValue()-1, id );
            try
            {
                idGenerator.nextId();
            }
            catch ( StoreFailureException e ) 
            { // good, capacity exceeded
            }
            idGenerator.close();
        }
        finally
        {
            File file = new File( idGeneratorFile() );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }
    
    @Test
    public void makeSureIdCapacityCannotBeExceeded() throws Exception
    {
        for ( IdType type : IdType.values() )
        {
            makeSureIdCapacityCannotBeExceeded( type );
        }
    }
    
    private void makeSureIdCapacityCannotBeExceeded( IdType type )
    {
        deleteIdGeneratorFile();
        IdGeneratorImpl.createGenerator( idGeneratorFile() );
        long maxValue = type.getMaxValue();
        IdGenerator idGenerator = new IdGeneratorImpl( idGeneratorFile(), 1, maxValue );
        long id = maxValue-2;
        idGenerator.setHighId( id );
        assertEquals( id, idGenerator.nextId() );
        assertEquals( id+1, idGenerator.nextId() );
        if ( maxValue != (long) Math.pow( 2, 32 )-1 )
        {
            // This is for the special -1 value
            assertEquals( id+2, idGenerator.nextId() );
        }
        try
        {
            idGenerator.nextId();
            fail( "Id capacity shouldn't be able to be exceeded for " + type );
        }
        catch ( StoreFailureException e )
        { // Good
        }
        idGenerator.close();
    }

    @Test
    public void makeSureMagicMinusOneIsntReturnedFromNodeIdGenerator() throws Exception
    {
        makeSureMagicMinusOneIsSkipped( IdType.NODE );
        makeSureMagicMinusOneIsSkipped( IdType.RELATIONSHIP );
        makeSureMagicMinusOneIsSkipped( IdType.PROPERTY );
    }

    private void makeSureMagicMinusOneIsSkipped( IdType type )
    {
        deleteIdGeneratorFile();
        IdGeneratorImpl.createGenerator( idGeneratorFile() );
        IdGenerator idGenerator = new IdGeneratorImpl( idGeneratorFile(), 1, type.getMaxValue() );
        long id = (long) Math.pow( 2, 32 )-3;
        idGenerator.setHighId( id );
        assertEquals( id, idGenerator.nextId() );
        assertEquals( id+1, idGenerator.nextId() );
        // Here we make sure that id+2 (integer -1) is skipped
        assertEquals( id+3, idGenerator.nextId() );
        assertEquals( id+4, idGenerator.nextId() );
        assertEquals( id+5, idGenerator.nextId() );
        idGenerator.close();
    }
    
    @Test
    public void makeSureMagicMinusOneCannotBeReturnedEvenIfFreed() throws Exception
    {
        IdGeneratorImpl.createGenerator( idGeneratorFile() );
        IdGenerator idGenerator = new IdGeneratorImpl( idGeneratorFile(), 1, IdType.NODE.getMaxValue() );
        long magicMinusOne = (long) Math.pow( 2, 32 )-1;
        idGenerator.setHighId( magicMinusOne );
        assertEquals( magicMinusOne+1, idGenerator.nextId() );
        idGenerator.freeId( magicMinusOne-1 );
        idGenerator.freeId( magicMinusOne );
        idGenerator.close();
        
        idGenerator = new IdGeneratorImpl( idGeneratorFile(), 1, IdType.NODE.getMaxValue() );
        assertEquals( magicMinusOne-1, idGenerator.nextId() );
        assertEquals( magicMinusOne+2, idGenerator.nextId() );
        idGenerator.close();
    }
    
    @Test
    public void commandsGetWrittenOnceSoThatFreedIdsGetsAddedOnlyOnce() throws Exception
    {
        String storeDir = "target/var/free-id-once";
        deleteRecursively( new File( storeDir ) );
        GraphDatabaseService db = new EmbeddedGraphDatabase( storeDir );
        RelationshipType type = withName( "SOME_TYPE" );
        Node rootNode = db.getReferenceNode();
        
        // This transaction will, if some commands may be executed more than once,
        // add the freed ids to the defrag list more than once - making the id generator
        // return the same id more than once during the next session.
        Set<Long> createdNodeIds = new HashSet<Long>();
        Set<Long> createdRelationshipIds = new HashSet<Long>();
        Transaction tx = db.beginTx();
        for ( int i = 0; i < 20; i++ )
        {
            Node otherNode = db.createNode();
            Relationship relationship = rootNode.createRelationshipTo( otherNode, type );
            if ( i%5 == 0 )
            {
                otherNode.delete();
                relationship.delete();
            }
            else
            {
                createdNodeIds.add( otherNode.getId() );
                createdRelationshipIds.add( relationship.getId() );
            }
        }
        tx.success();
        tx.finish();
        db.shutdown();
        
        // After a clean shutdown, create new nodes and relationships and see so that
        // all ids are unique.
        db = new EmbeddedGraphDatabase( storeDir );
        rootNode = db.getReferenceNode();
        tx = db.beginTx();
        for ( int i = 0; i < 100; i++ )
        {
            Node otherNode = db.createNode();
            if ( !createdNodeIds.add( otherNode.getId() ) )
            {
                fail( "Managed to create a node with an id that was already in use" );
            }
            Relationship relationship = rootNode.createRelationshipTo( otherNode, type );
            if ( !createdRelationshipIds.add( relationship.getId() ) )
            {
                fail( "Managed to create a relationship with an id that was already in use" );
            }
        }
        tx.success();
        tx.finish();
        
        // Verify by loading everything from scratch
        ((AbstractGraphDatabase)db).getConfig().getGraphDbModule().getNodeManager().clearCache();
        for ( Node node : db.getAllNodes() )
        {
            lastOrNull( node.getRelationships() );
        }
        
        db.shutdown();
    }
    
    @Test
    public void clearFreeIds()
    {
        IdGeneratorImpl.createGenerator( idGeneratorFile() );
        IdGenerator generator = new IdGeneratorImpl( idGeneratorFile(), 10, 100 );
        
        // Create then free enough ids to make #freeId write out id batches which we rely on
        // #clearFreeIds to clear out, as well as the state in memory.
        long[] ids = new long[35];
        for ( int i = 0; i < ids.length; i++ ) ids[i] = generator.nextId();
        for ( long id : ids ) generator.freeId( id ); 
        long nextExpectedId = ids[ids.length-1]+1;
        generator.close();
        generator = new IdGeneratorImpl( idGeneratorFile(), 10, 100 );
        generator.clearFreeIds();
        assertEquals( nextExpectedId++, generator.nextId() );
        generator.close();
        
        generator = new IdGeneratorImpl( idGeneratorFile(), 10, 100 );
        assertEquals( nextExpectedId, generator.nextId() );
        for ( int i = 0; i < ids.length; i++ ) ids[i] = generator.nextId();
        for ( long id : ids ) generator.freeId( id ); 
        nextExpectedId = ids[ids.length-1]+1;
        generator.close();
        generator = new IdGeneratorImpl( idGeneratorFile(), 10, 100 );
        generator.clearFreeIds();
        assertEquals( nextExpectedId++, generator.nextId() );
        generator.close();
    }
}
