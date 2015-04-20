/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Settings;
import org.neo4j.io.fs.FileLock;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.StoreLocker;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class TestOsSpecificLocks
{
    private File path;

    @Rule
    public TestName name = new TestName();

    @Before
    public void doBefore()
    {
        path = TargetDirectory.forTest( getClass() ).cleanDirectory( name.getMethodName() );
    }

    @Test
    public void sanityCheck() throws Exception
    {
        assumeTrue( Settings.osIsWindows() );
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        // Must grab locks only on store_lock file
        File fileName = new File( path, StoreLocker.STORE_LOCK_FILENAME );
        StoreChannel channel = fs.open( fileName, "rw" );
        // Lock this sucker!
        FileLock lock = fs.tryLock( fileName, channel );
        assertTrue( new File( path, "lock" ).exists() );
        
        try
        {
            fs.tryLock( fileName, channel );
            fail( "Should have thrown IOException" );
        }
        catch ( IOException e )
        {   // Good, expected
        }

        // But the rest of the files should return non null (placebos,
        // actually)
        StoreChannel tempChannel = fs.open( new File( fileName.getPath() + "1" ), "rw" );
        FileLock tempLock = fs.tryLock( new File( fileName.getPath() + "1"), tempChannel );
        assertNotNull( tempLock );
        tempLock.release();
        tempChannel.close();

        // Release and retry, should succeed
        lock.release();
        assertFalse( new File( path, "lock" ).exists() );
        fs.tryLock( fileName, channel ).release(); // NPE on fail here
        assertFalse( new File( path, "lock" ).exists() );
    }

    @Test
    public void testDatabaseLocking()
    {
        assumeTrue( Settings.osIsWindows() );
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( path.getPath() );
        Transaction tx = db.beginTx();
        db.createNode();
        tx.success();
        tx.finish();
        assertTrue( new File( path, "lock" ).exists() );
        try
        {
            new TestGraphDatabaseFactory().newEmbeddedDatabase( path.getPath() );
            fail("Should not be able to start up another db in the same dir");
        }
        catch ( Exception e )
        {
            // Good
        }
        db.shutdown();
    }
}
