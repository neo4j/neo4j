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
package org.neo4j.kernel.impl.api;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.ImpermanentGraphDatabase;

public class KernelTest
{
    @Test
    public void readOnlyStatementContextLifecycleShouldBeThredSafe() throws Exception
    {
        // GIVEN
        ImpermanentGraphDatabase db = new ImpermanentGraphDatabase();
        KernelAPI kernel = db.getDependencyResolver().resolveDependency( KernelAPI.class );
        DoubleLatch latch = new DoubleLatch( 10 );
        List<Worker> workers = new ArrayList<Worker>();
        for ( int i = 0; i < latch.getNumberOfContestants(); i++ )
        {
            workers.add( new Worker( kernel, latch ) );
        }
        latch.awaitFinish();

        // WHEN

        // THEN
        for ( Worker worker : workers )
        {
            if ( worker.failure != null )
            {
                throw new RuntimeException( "Worker failed", worker.failure );
            }
        }
        db.shutdown();
    }

    @Test
    public void shouldNotAllowCreationOfConstraintsWhenInHA() throws Exception
    {
        GraphDatabaseAPI db = new FakeHaDatabase();
        KernelAPI kernelAPI = db.getDependencyResolver().resolveDependency( KernelAPI.class );
        db.beginTx();
        TransactionContext tx = kernelAPI.newTransactionContext();
        StatementContext ctx = tx.newStatementContext();
        try
        {
            ctx.addUniquenessConstraint( 1, 1 );
            fail("expected exception here");
        }
        catch ( UnsupportedSchemaModificationException e )
        { //Good
        }
        db.shutdown();
    }

    class FakeHaDatabase extends ImpermanentGraphDatabase
    {
        @Override
        protected boolean isHighlyAvailable()
        {
            return true;
        }
    }

    private static class Worker extends Thread
    {
        private final KernelAPI kernel;
        private final DoubleLatch latch;
        volatile Throwable failure;

        Worker( KernelAPI kernel, DoubleLatch latch )
        {
            this.kernel = kernel;
            this.latch = latch;
            start();
        }

        @Override
        public void run()
        {
            try
            {
                latch.start();
                StatementContext statement = kernel.newReadOnlyStatementContext();
                statement.close();
            }
            catch ( Throwable e )
            {
                failure = e;
            }
            finally
            {
                latch.finish();
            }
        }
    }
}
