/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.rule.OtherThreadRule;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Label.label;

@ImpermanentDbmsExtension
@ExtendWith( OtherThreadExtension.class )
public class UniquenessConstraintValidationConcurrencyIT
{
    @Inject
    private GraphDatabaseService database;
    @Inject
    private OtherThreadRule otherThread;

    @Test
    void shouldAllowConcurrentCreationOfNonConflictingData() throws Exception
    {
        createTestConstraint();

        try ( var tx = database.beginTx() )
        {
            tx.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );
            assertTrue( otherThread.execute( createNode( "Label1", "key1", "value2" ) ).get() );
        }
    }

    @Test
    void shouldPreventConcurrentCreationOfConflictingData() throws Exception
    {
        createTestConstraint();

        Future<Boolean> result;
        try ( var tx = database.beginTx() )
        {
            tx.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );
            try
            {
                result = otherThread.execute( createNode( "Label1", "key1", "value1" ) );
            }
            finally
            {
                waitUntilWaiting();
            }
            tx.commit();
        }
        assertFalse( result.get(), "node creation should fail" );
    }

    @Test
    void shouldAllowOtherTransactionToCompleteIfFirstTransactionRollsBack() throws Exception
    {
        createTestConstraint();

        // when
        Future<Boolean> result;
        try ( var tx = database.beginTx() )
        {
            tx.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );
            try
            {
                result = otherThread.execute( createNode( "Label1", "key1", "value1" ) );
            }
            finally
            {
                waitUntilWaiting();
            }
            tx.rollback();
        }
        assertTrue( result.get(), "node creation should fail" );
    }

    private void createTestConstraint()
    {
        try ( var transaction = database.beginTx() )
        {
            transaction.schema().constraintFor( label( "Label1" ) ).assertPropertyIsUnique( "key1" ).create();
            transaction.commit();
        }
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

    public Callable<Boolean> createNode( final String label, final String propertyKey, final Object propertyValue )
    {
        return () ->
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
