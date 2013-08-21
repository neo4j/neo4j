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

import java.util.concurrent.Future;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Function;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.rules.RuleChain.outerRule;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.test.OtherThreadRule.isBlocked;

public class UniquenessConstraintValidationConcurrencyIT
{
    @Rule
    public TestRule evaluationOrder()
    {
        return outerRule( database ).around( otherThread );
    }

    private final DatabaseRule database = new ImpermanentDatabaseRule();
    private final OtherThreadRule<GraphDatabaseService> otherThread = new OtherThreadRule<GraphDatabaseService>()
    {
        @Override
        protected GraphDatabaseService initialState()
        {
            return database.getGraphDatabaseService();
        }
    };

    @Test
    public void shouldAllowConcurrentCreationOfNonConflictingData() throws Exception
    {
        // given
        database.transaction( createUniquenessConstraint( "Label1", "key1" ) );

        // when
        Future<Boolean> created = database.transaction( new Function<GraphDatabaseService, Future<Boolean>>()
        {
            @Override
            public Future<Boolean> apply( GraphDatabaseService db )
            {
                db.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );
                return otherThread.execute( createNode( "Label1", "key1", "value2" ) );
            }
        } );

        // then
        assertTrue( "Node creation should succeed", created.get() );
    }

    @Test
    public void shouldPreventConcurrentCreationOfConflictingData() throws Exception
    {
        // given
        database.transaction( createUniquenessConstraint( "Label1", "key1" ) );

        // when
        Future<Boolean> created = database.transaction( new Function<GraphDatabaseService, Future<Boolean>>()
        {
            @Override
            public Future<Boolean> apply( GraphDatabaseService db )
            {
                db.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );
                try
                {
                    return otherThread.execute( createNode( "Label1", "key1", "value1" ) );
                }
                finally
                {
                    assertThat( otherThread, isBlocked() );
                }
            }
        } );

        // then
        assertFalse( "node creation should fail", created.get() );
    }

    @Test
    public void shouldAllowOtherTransactionToCompleteIfFirstTransactionRollsBack() throws Exception
    {
        // given
        database.transaction( createUniquenessConstraint( "Label1", "key1" ) );

        // when
        Future<Boolean> created = database.failingTransaction( new Function<GraphDatabaseService, Future<Boolean>>()
        {
            @Override
            public Future<Boolean> apply( GraphDatabaseService db )
            {
                db.createNode( label( "Label1" ) ).setProperty( "key1", "value1" );
                try
                {
                    return otherThread.execute( createNode( "Label1", "key1", "value1" ) );
                }
                finally
                {
                    assertThat( otherThread, isBlocked() );
                }
            }
        } );

        // then
        assertTrue( "Node creation should succeed", created.get() );
    }

    private static Function<GraphDatabaseService, Void> createUniquenessConstraint(
            final String label, final String propertyKey )
    {
        return new Function<GraphDatabaseService, Void>()
        {
            @Override
            public Void apply( GraphDatabaseService db )
            {
                db.schema().constraintFor( label( label ) ).unique().on( propertyKey ).create();
                return null;
            }
        };
    }

    private static OtherThreadExecutor.WorkerCommand<GraphDatabaseService, Boolean> createNode(
            final String label, final String propertyKey, final Object propertyValue )
    {
        return new OtherThreadExecutor.WorkerCommand<GraphDatabaseService, Boolean>()
        {
            @Override
            public Boolean doWork( GraphDatabaseService db ) throws Exception
            {
                Transaction tx = db.beginTx();
                try
                {
                    db.createNode( label( label ) ).setProperty( propertyKey, propertyValue );

                    tx.success();
                    return true;
                }
                catch ( ConstraintViolationException e )
                {
                    return false;
                }
                finally
                {
                    tx.finish();
                }
            }
        };
    }
}
