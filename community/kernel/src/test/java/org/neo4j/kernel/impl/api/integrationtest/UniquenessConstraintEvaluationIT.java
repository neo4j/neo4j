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
package org.neo4j.kernel.impl.api.integrationtest;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.ConstraintCreationException;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.impl.api.ConstraintCreationKernelException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicLabel.label;

public class UniquenessConstraintEvaluationIT extends KernelIntegrationTest
{
    @Test
    public void shouldAbortConstraintCreationWhenDuplicatesExist() throws Exception
    {
        // given
        newTransaction();
        // name is not unique for Foo in the existing data
        db.createNode( label( "Foo" ) ).setProperty( "name", "foo" );
        db.createNode( label( "Foo" ) ).setProperty( "name", "foo" );
        long foo = statement.getLabelId( "Foo" );
        long name = statement.getPropertyKeyId( "name" );
        commit();

        newTransaction();

        // when
        try
        {
            statement.addUniquenessConstraint( foo, name );

            fail( "expected exception" );
        }
        // then
        catch ( ConstraintCreationKernelException ex )
        {
            assertEquals( new UniquenessConstraint( foo, name ), ex.constraint() );
            // TODO: assert about the exception we threw...
            assertThat( ex.getCause().getCause(), instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    @Ignore("2013-05-13 Not passing yet - requires work on how to hand over the constraint from pre-phase to live.\n" +
            "Specifically, we need to be able to fail the transaction if the constraint doesn't hold, have that " +
            "exception propagate nicely without setting the TM in a bad state.\n" +
            "Ideas for this currently revolve around doing the final constraint evaluation in prepare() and lock the " +
            "schema (or the index? - easier, but requires the indexing to be synchronous) to prevent changes until " +
            "the constraint is live and then make the constraint live and release the locks as part of commit().\n" +
            "Possibly there is something that can be done with IndexProxy and more states in the flipping impl, make " +
            "the index enter a state where it operates as if online from an index POV, but adds failures to a queue, " +
            "this would make the operations synchronous from the indexing POV but the failures would be async, " +
            "reported when committing the constraint creating transaction.")
    public void shouldFailOnCommitIfConstraintIsBrokenAfterConstraintAdded() throws Exception
    {
        // given
        newTransaction();
        db.createNode( label( "Foo" ) ).setProperty( "name", "foo" );
        long foo = statement.getLabelId( "Foo" );
        long name = statement.getPropertyKeyId( "name" );
        commit();

        newTransaction();
        statement.addUniquenessConstraint( foo, name );
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit( new Runnable()
        {
            @Override
            public void run()
            {
                Transaction tx = db.beginTx();
                try
                {
                    db.createNode( label( "Foo" ) ).setProperty( "name", "foo" );
                    tx.success();
                }
                finally
                {
                    tx.finish();
                }
            }
        } ).get();
        executor.shutdown();

        // when
        try
        {
            commit();

            fail( "expected exception" );
        }
        // then
        catch ( ConstraintCreationException ex )
        {
            assertEquals( "", ex.getMessage() );
        }
    }

    @Test
    @Ignore("2013-05-13 This is to be supported when we implement enforcing constraints")
    public void shouldEnforceUniquenessConstraint() throws Exception
    {
        // given
        newTransaction();
        db.createNode( label( "Foo" ) ).setProperty( "name", "foo" );
        long foo = statement.getLabelId( "Foo" );
        long name = statement.getPropertyKeyId( "name" );
        commit();
        newTransaction();
        statement.addUniquenessConstraint( foo, name );
        commit();

        newTransaction();

        // when
        try
        {
            db.createNode( label( "Foo" ) ).setProperty( "name", "foo" );

            fail( "expected exception" );
        }
        // then
        catch ( Exception ex ) // TODO: narrow this catch clause
        {
            ex.printStackTrace();
            assertNotNull( ex );
        }
    }
}
