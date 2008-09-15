/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.nioneo.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.neo4j.impl.nioneo.store.IdGenerator;

public class TestIdGenerator extends TestCase
{

    public TestIdGenerator( String testName )
    {
        super( testName );
    }

    public static void main( java.lang.String[] args )
    {
        junit.textui.TestRunner.run( suite() );
    }

    public static Test suite()
    {
        TestSuite suite = new TestSuite( TestIdGenerator.class );
        return suite;
    }

    public void setUp()
    {
    }

    public void tearDown()
    {
    }

    public void testCreateIdGenerator()
    {
        try
        {
            IdGenerator.createGenerator( null );
            fail( "Null filename should throw exception" );
        }
        catch ( IllegalArgumentException e )
        {
        } // good
        try
        {
            IdGenerator.createGenerator( "testIdGenerator.id" );
            new IdGenerator( "testIdGenerator.id", 0 ).close();
            fail( "Zero grab size should throw exception" );
        }
        catch ( IllegalArgumentException e )
        {
        } // good
        try
        {
            new IdGenerator( "testIdGenerator.id", -1 ).close();
            fail( "Negative grab size should throw exception" );
        }
        catch ( IllegalArgumentException e )
        {
        } // good

        try
        {
            IdGenerator idGenerator = new IdGenerator( "testIdGenerator.id",
                1008 );
            try
            {
                IdGenerator.createGenerator( "testIdGenerator.id" );
                fail( "Creating a id generator with existing file name "
                    + "should throw exception" );
            }
            catch ( IllegalStateException e )
            {
            } // good
            idGenerator.close();
            // verify that id generator is ok
            FileChannel fileChannel = new FileInputStream( "testIdGenerator.id" )
                .getChannel();
            ByteBuffer buffer = ByteBuffer.allocate( 5 );
            assertEquals( 5, fileChannel.read( buffer ) );
            buffer.flip();
            assertEquals( (byte) 0, buffer.get() );
            assertEquals( 0, buffer.getInt() );
            buffer.flip();
            int readCount = fileChannel.read( buffer );
            if ( readCount != -1 && readCount != 0 )
            {
                fail( "Id generator header not ok read 5 + " + readCount
                    + " bytes from file" );
            }
            fileChannel.close();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            fail( "" + e );
        }
        finally
        {
            File file = new File( "testIdGenerator.id" );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    public void testStickyGenerator()
    {
        try
        {
            IdGenerator.createGenerator( "testIdGenerator.id" );
            IdGenerator idGen = new IdGenerator( "testIdGenerator.id", 3 );
            try
            {
                new IdGenerator( "testIdGenerator.id", 3 );
                fail( "Opening sticky id generator should throw exception" );
            }
            catch ( StoreFailureException e )
            { // good
            }
            idGen.close();
        }
        finally
        {
            File file = new File( "testIdGenerator.id" );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    public void testNextId()
    {
        try
        {
            IdGenerator.createGenerator( "testIdGenerator.id" );
            IdGenerator idGenerator = new IdGenerator( "testIdGenerator.id", 3 );
            for ( int i = 0; i < 7; i++ )
            {
                assertEquals( i, idGenerator.nextId() );
            }
            idGenerator.freeId( 1 );
            idGenerator.freeId( 3 );
            idGenerator.freeId( 5 );
            assertEquals( 7, idGenerator.nextId() );
            idGenerator.freeId( 6 );
            idGenerator.close();
            idGenerator = new IdGenerator( "testIdGenerator.id", 5 );
            idGenerator.freeId( 2 );
            idGenerator.freeId( 4 );
            assertEquals( 1, idGenerator.nextId() );
            idGenerator.freeId( 1 );
            assertEquals( 3, idGenerator.nextId() );
            idGenerator.freeId( 3 );
            assertEquals( 5, idGenerator.nextId() );
            idGenerator.freeId( 5 );
            assertEquals( 6, idGenerator.nextId() );
            idGenerator.freeId( 6 );
            assertEquals( 8, idGenerator.nextId() );
            idGenerator.freeId( 8 );
            assertEquals( 9, idGenerator.nextId() );
            idGenerator.freeId( 9 );
            idGenerator.close();
            idGenerator = new IdGenerator( "testIdGenerator.id", 3 );
            assertEquals( 2, idGenerator.nextId() );
            assertEquals( 4, idGenerator.nextId() );
            assertEquals( 1, idGenerator.nextId() );
            assertEquals( 3, idGenerator.nextId() );
            assertEquals( 5, idGenerator.nextId() );
            assertEquals( 6, idGenerator.nextId() );
            assertEquals( 8, idGenerator.nextId() );
            assertEquals( 9, idGenerator.nextId() );
            assertEquals( 10, idGenerator.nextId() );
            assertEquals( 11, idGenerator.nextId() );
            idGenerator.close();
        }
        finally
        {
            File file = new File( "testIdGenerator.id" );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    public void testFreeId()
    {
        try
        {
            IdGenerator.createGenerator( "testIdGenerator.id" );
            IdGenerator idGenerator = new IdGenerator( "testIdGenerator.id", 3 );
            for ( int i = 0; i < 7; i++ )
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
            idGenerator = new IdGenerator( "testIdGenerator.id", 2 );
            assertEquals( 0, idGenerator.nextId() );
            assertEquals( 1, idGenerator.nextId() );
            assertEquals( 2, idGenerator.nextId() );
            idGenerator.close();
            idGenerator = new IdGenerator( "testIdGenerator.id", 2 );
            assertEquals( 4, idGenerator.nextId() );
            assertEquals( 5, idGenerator.nextId() );
            assertEquals( 6, idGenerator.nextId() );
            assertEquals( 3, idGenerator.nextId() );
            idGenerator.close();
        }
        finally
        {
            File file = new File( "testIdGenerator.id" );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    public void testClose()
    {
        try
        {
            IdGenerator.createGenerator( "testIdGenerator.id" );
            IdGenerator idGenerator = new IdGenerator( "testIdGenerator.id", 2 );
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
            catch ( IllegalArgumentException e )
            { // good
            }
            idGenerator = new IdGenerator( "testIdGenerator.id", 2 );
            assertEquals( 0, idGenerator.nextId() );
            assertEquals( 1, idGenerator.nextId() );
            assertEquals( 2, idGenerator.nextId() );
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
            catch ( IllegalArgumentException e )
            { // good
            }
        }
        finally
        {
            File file = new File( "testIdGenerator.id" );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    public void testOddAndEvenWorstCase()
    {
        int capacity = 1024 * 8 + 1;
        try
        {
            IdGenerator.createGenerator( "testIdGenerator.id" );
            IdGenerator idGenerator = new IdGenerator( "testIdGenerator.id",
                128 );
            for ( int i = 0; i < capacity; i++ )
            {
                idGenerator.nextId();
            }
            java.util.Map<Integer,Object> freedIds = new java.util.HashMap<Integer,Object>();
            for ( int i = 1; i < capacity; i += 2 )
            {
                idGenerator.freeId( i );
                freedIds.put( new Integer( i ), this );
            }
            idGenerator.close();
            idGenerator = new IdGenerator( "testIdGenerator.id", 2000 );
            int oldId = -1;
            for ( int i = 0; i < capacity - 1; i += 2 )
            {
                int id = idGenerator.nextId();
                if ( freedIds.remove( new Integer( id ) ) == null )
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
            File file = new File( "testIdGenerator.id" );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
        try
        {
            IdGenerator.createGenerator( "testIdGenerator.id" );
            IdGenerator idGenerator = new IdGenerator( "testIdGenerator.id",
                128 );
            for ( int i = 0; i < capacity; i++ )
            {
                idGenerator.nextId();
            }
            java.util.Map<Integer,Object> freedIds = new java.util.HashMap<Integer,Object>();
            for ( int i = 0; i < capacity; i += 2 )
            {
                idGenerator.freeId( i );
                freedIds.put( new Integer( i ), this );
            }
            idGenerator.close();
            idGenerator = new IdGenerator( "testIdGenerator.id", 2000 );
            for ( int i = 0; i < capacity; i += 2 )
            {
                assertEquals( this, freedIds.remove( new Integer( idGenerator
                    .nextId() ) ) );
            }
            assertEquals( 0, freedIds.values().size() );
            idGenerator.close();
        }
        finally
        {
            File file = new File( "testIdGenerator.id" );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    public void testRandomTest()
    {
        int numberOfCloses = 0;
        java.util.Random random = new java.util.Random( System
            .currentTimeMillis() );
        int capacity = random.nextInt( 1024 ) + 1024;
        int grabSize = random.nextInt( 128 ) + 128;
        IdGenerator.createGenerator( "testIdGenerator.id" );
        IdGenerator idGenerator = new IdGenerator( "testIdGenerator.id",
            grabSize );
        java.util.ArrayList<Integer> idsTaken = new java.util.ArrayList<Integer>();
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
                    idsTaken.add( new Integer( idGenerator.nextId() ) );
                    currentIdCount++;
                }
                if ( rIndex > (1.0f - closeIndex) || rIndex < closeIndex )
                {
                    idGenerator.close();
                    grabSize = random.nextInt( 128 ) + 128;
                    idGenerator = new IdGenerator( "testIdGenerator.id",
                        grabSize );
                    numberOfCloses++;
                }
            }
            idGenerator.close();
        }
        finally
        {
            File file = new File( "testIdGenerator.id" );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }

    }
}