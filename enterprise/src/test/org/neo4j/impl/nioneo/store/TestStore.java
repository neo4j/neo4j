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
import java.io.IOException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.neo4j.impl.nioneo.store.AbstractStore;

public class TestStore extends TestCase
{

    public TestStore( String testName )
    {
        super( testName );
    }

    public static void main( java.lang.String[] args )
    {
        junit.textui.TestRunner.run( suite() );
    }

    public static Test suite()
    {
        TestSuite suite = new TestSuite( TestStore.class );
        return suite;
    }

    public void setUp()
    {
    }

    public void tearDown()
    {
    }

    public void testCreateStore()
    {
        try
        {
            try
            {
                Store.createStore( null );
                fail( "Null fileName should throw exception" );
            }
            catch ( IllegalArgumentException e )
            { // good
            }
            Store store = Store.createStore( "testStore.db" );
            try
            {
                Store.createStore( "testStore.db" );
                fail( "Creating existing store should throw exception" );
            }
            catch ( IllegalStateException e )
            { // good
            }
            store.close();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            fail( "" + e );
        }
        finally
        {
            File file = new File( "testStore.db" );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
            file = new File( "testStore.db.id" );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    public void testStickyStore()
    {
        try
        {
            Store.createStore( "testStore.db" ).close();
            java.nio.channels.FileChannel fileChannel = new java.io.RandomAccessFile(
                "testStore.db", "rw" ).getChannel();
            fileChannel.truncate( fileChannel.size() - 2 );
            fileChannel.close();
            Store store = new Store( "testStore.db" );
            store.makeStoreOk();
            store.close();
        }
        catch ( IOException e )
        {
            fail( "" + e );
        }
        finally
        {
            File file = new File( "testStore.db" );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
            file = new File( "testStore.db.id" );
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
            Store store = Store.createStore( "testStore.db" );
            store.close();
        }
        catch ( IOException e )
        {
            fail( "" + e );
        }
        finally
        {
            File file = new File( "testStore.db" );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
            file = new File( "testStore.db.id" );
            if ( file.exists() )
            {
                assertTrue( file.delete() );
            }
        }
    }

    private static class Store extends AbstractStore
    {
        // store version, each store ends with this string (byte encoded)
        private static final String VERSION = "TestVersion v0.1";
        private static final int RECORD_SIZE = 1;

        public Store( String fileName ) throws IOException
        {
            super( fileName );
        }

        protected void initStorage()
        {
        }

        protected void closeImpl()
        {
        }

        protected boolean fsck( boolean modify )
        {
            return false;
        }

        public int getRecordSize()
        {
            return RECORD_SIZE;
        }

        public String getTypeAndVersionDescriptor()
        {
            return VERSION;
        }

        public static Store createStore( String fileName ) throws IOException
        {
            createEmptyStore( fileName, VERSION );
            return new Store( fileName );
        }

        public void flush()
        {
        }

        protected void rebuildIdGenerator()
        {
        }
    }
}