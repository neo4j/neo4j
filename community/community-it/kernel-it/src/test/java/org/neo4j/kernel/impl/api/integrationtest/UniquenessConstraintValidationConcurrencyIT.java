/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.test.rule.OtherThreadRule;

import static java.lang.Thread.State.WAITING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;

public class UniquenessConstraintValidationConcurrencyIT
{
    @Rule
    public final DbmsRule database = new ImpermanentDbmsRule();
    @Rule
    public final OtherThreadRule<Void> otherThread = new OtherThreadRule<>();

    @Test
    public void shouldAllowConcurrentCreationOfNonConflictingData() throws Exception
    {
        // given
        database.executeAndCommit( createUniquenessConstraint( "Label1", "key1" ) );

        // when
        Future<Boolean> created = database.executeAndCommit( tx ->
        {
            tx.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );
            return otherThread.execute( createNode( tx, "Label1", "key1", "value2" ) );
        } );

        // then
        assertTrue( "Node creation should succeed", created.get() );
    }

    @Test
    public void shouldPreventConcurrentCreationOfConflictingData() throws Exception
    {
        // given
        database.executeAndCommit( createUniquenessConstraint( "Label1", "key1" ) );

        // when
        Future<Boolean> created = database.executeAndCommit( tx ->
        {
            tx.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );
            try
            {
                return otherThread.execute( createNode( tx, "Label1", "key1", "value1" ) );
            }
            finally
            {
                waitUntilWaiting();
            }
        } );

        // then
        assertFalse( "node creation should fail", created.get() );
    }

    @Test
    public void shouldAllowOtherTransactionToCompleteIfFirstTransactionRollsBack() throws Exception
    {
        // given
        database.executeAndCommit( createUniquenessConstraint( "Label1", "key1" ) );

        // when
        Future<Boolean> created = database.executeAndRollback( tx ->
        {
            tx.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );
            try
            {
                return otherThread.execute( createNode( tx, "Label1", "key1", "value1" ) );
            }
            finally
            {
                waitUntilWaiting();
            }
        } );

        // then
        assertTrue( "Node creation should succeed", created.get() );
    }

    private void waitUntilWaiting()
    {
        try
        {
            otherThread.get().waitUntilWaiting();
        }
        catch ( TimeoutException e )
        {
            throw new RuntimeException( e );
        }
    }

    private Function<Transaction, Void> createUniquenessConstraint( final String label, final String propertyKey )
    {
        return transaction ->
        {
            transaction.schema().constraintFor( label( label ) ).assertPropertyIsUnique( propertyKey ).create();
            return null;
        };
    }

    public OtherThreadExecutor.WorkerCommand<Void, Boolean> createNode(
            final Transaction transaction, final String label, final String propertyKey, final Object propertyValue )
    {
        return nothing ->
        {
            try ( Transaction tx = database.beginTx() )
            {
                tx.createNode( label( label ) ).setProperty( propertyKey, propertyValue );

                tx.commit();
                return true;
            }
            catch ( ConstraintViolationException e )
            {
                return false;
            }
        };
    }
}
