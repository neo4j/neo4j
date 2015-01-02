/**
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

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.mockfs.LimitedFileSystemGraphDatabase;
import org.neo4j.helpers.Exceptions;
import org.neo4j.test.CleanupRule;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
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
        LimitedFileSystemGraphDatabase db = cleanup.add( new LimitedFileSystemGraphDatabase() );

        db.runOutOfDiskSpaceNao();

        // When
        Transaction tx = db.beginTx();
        db.createNode();
        tx.success();

        try
        {
            tx.finish();
            fail( "Expected tx finish to throw TransactionFailureException when filesystem is full." );
        }
        catch ( TransactionFailureException e )
        {
            exceptionThrown = e;
        }

        // Then
        assertTrue( Exceptions.contains( exceptionThrown, IOException.class ) );
        db.somehowGainMoreDiskSpace(); // to help shutting down the db
    }

    @Test
    public void shouldStopDatabaseWhenOutOfDiskSpace() throws Exception
    {
        // Given
        TransactionFailureException errorCaught = null;
        LimitedFileSystemGraphDatabase db = cleanup.add( new LimitedFileSystemGraphDatabase() );

        db.runOutOfDiskSpaceNao();

        Transaction tx = db.beginTx();
        db.createNode();
        tx.success();

        try
        {
            tx.finish();
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
    }

    public final @Rule CleanupRule cleanup = new CleanupRule();
}
