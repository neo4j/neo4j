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
package org.neo4j.graphdb;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.mockfs.LimitedFileSystemGraphDatabase;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.record.NeoStoreUtil;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RunOutOfDiskSpaceIT
{
    @Test
    public void shouldPropagateIOExceptions() throws Exception
    {
        // Given
        TransactionFailureException exceptionThrown = null;

        String storeDir = testDirectory.absolutePath();
        LimitedFileSystemGraphDatabase db = new LimitedFileSystemGraphDatabase( storeDir );

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        long logVersion = db.getDependencyResolver().resolveDependency( NeoStore.class ).getCurrentLogVersion();

        db.runOutOfDiskSpaceNao();

        // When
        Transaction tx = db.beginTx();
        db.createNode();
        tx.success();

        try
        {
            tx.close();
            fail( "Expected tx finish to throw TransactionFailureException when filesystem is full." );
        }
        catch ( TransactionFailureException e )
        {
            exceptionThrown = e;
        }

        // Then
        assertTrue( Exceptions.contains( exceptionThrown, IOException.class ) );

        db.somehowGainMoreDiskSpace(); // to help shutting down the db
        db.shutdown();

        assertEquals( logVersion, new NeoStoreUtil( new File( storeDir ), db.getFileSystem() ).getLogVersion() );
    }

    @Test
    public void shouldStopDatabaseWhenOutOfDiskSpace() throws Exception
    {
        // Given
        TransactionFailureException errorCaught = null;

        String storeDir = testDirectory.absolutePath();
        LimitedFileSystemGraphDatabase db = new LimitedFileSystemGraphDatabase( storeDir );

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        long logVersion = db.getDependencyResolver().resolveDependency( NeoStore.class ).getCurrentLogVersion();

        db.runOutOfDiskSpaceNao();

        Transaction tx = db.beginTx();
        db.createNode();
        tx.success();

        try
        {
            tx.close();
            fail( "Expected tx finish to throw TransactionFailureException when filesystem is full." );
        }
        catch ( TransactionFailureException e )
        {
            // Expected
        }

        // When
        try
        {
            db.beginTx();
            fail( "Expected tx begin to throw TransactionFailureException when tx manager breaks." );
        }
        catch ( TransactionFailureException e )
        {
            errorCaught = e;
        }

        // Then
        assertThat( errorCaught.getCause(), is( instanceOf( org.neo4j.kernel.api.exceptions.TransactionFailureException.class ) ) );

        db.somehowGainMoreDiskSpace(); // to help shutting down the db
        db.shutdown();

        assertEquals( logVersion, new NeoStoreUtil( new File( storeDir ), db.getFileSystem() ).getLogVersion() );
    }

    public final @Rule TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );
}
