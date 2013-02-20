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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.nio.channels.FileChannel;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.StoreLocker;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

public class TestOsSpecificLocks
{
    private String path;
    
    @Before
    public void doBefore()
    {
        path = AbstractNeo4jTestCase.getStorePath( "checkLocks" );
        AbstractNeo4jTestCase.deleteFileOrDirectory( path );
        new File( path ).mkdirs();
    }
    
    @Test
    public void sanityCheck() throws Exception
    {
        assumeTrue( GraphDatabaseSetting.osIsWindows() );
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        // Must end in store_lock to get the lock
        String fileName = path + "\\"+ StoreLocker.STORE_LOCK_FILENAME;
        FileChannel channel = fs.open( fileName, "rw" );
        // Lock this sucker!
        FileLock lock = fs.tryLock( fileName, channel );
        assertTrue( new File( path + "\\lock" ).exists() );
        // If we try to lock with the lock held, a null should be served
        assertNull( fs.tryLock( fileName, channel ) );

        // But the rest of the files should return non null (placebos,
        // actually)
        FileChannel tempChannel = fs.open( fileName + "1", "rw" );
        FileLock tempLock = fs.tryLock( fileName + "1", tempChannel );
        assertNotNull( tempLock );
        tempLock.release();
        tempChannel.close();

        // Release and retry, should succeed
        lock.release();
        assertFalse( new File( path + "\\lock" ).exists() );
        fs.tryLock( fileName, channel ).release(); // NPE on fail here
        assertFalse( new File( path + "\\lock" ).exists() );
    }

    @Test
    public void testDatabaseLocking()
    {
        assumeTrue( GraphDatabaseSetting.osIsWindows() );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( path );
        Transaction tx = db.beginTx();
        db.createNode();
        tx.success();
        tx.finish();
        assertTrue( new File( path + "\\lock" ).exists() );
        try
        {
            new GraphDatabaseFactory().newEmbeddedDatabase( path );
            fail("Should not be able to start up another db in the same dir");
        }
        catch ( Exception e )
        {
            // Good
        }
        db.shutdown();
    }
}
