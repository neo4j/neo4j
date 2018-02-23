/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.function.Consumer;
import javax.annotation.Resource;

import org.neo4j.test.extension.EmbeddedDatabaseExtension;
import org.neo4j.test.rule.EmbeddedDatabaseRule;

import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith( EmbeddedDatabaseExtension.class )
public abstract class AbstractMandatoryTransactionsTest<T>
{
    @Resource
    public EmbeddedDatabaseRule dbRule;

    public T obtainEntity()
    {
        GraphDatabaseService graphDatabaseService = dbRule.getGraphDatabaseAPI();

        try ( Transaction tx = graphDatabaseService.beginTx() )
        {
            T result = obtainEntityInTransaction( graphDatabaseService );
            tx.success();

            return result;
        }
    }

    public void obtainEntityInTerminatedTransaction( Consumer<T> f )
    {
        GraphDatabaseService graphDatabaseService = dbRule.getGraphDatabaseAPI();

        try ( Transaction tx = graphDatabaseService.beginTx() )
        {
            T result = obtainEntityInTransaction( graphDatabaseService );
            tx.terminate();

            f.accept(result);
        }
    }

    protected abstract T obtainEntityInTransaction( GraphDatabaseService graphDatabaseService );

    public static <T> void assertFacadeMethodsThrowNotInTransaction( T entity, Iterable<FacadeMethod<T>> methods )
    {
        for ( FacadeMethod<T> method : methods )
        {
            try
            {
                method.call( entity );

                fail( "Transactions are mandatory, also for reads: " + method );
            }
            catch ( NotInTransactionException e )
            {
                // awesome
            }
        }
    }

    public void assertFacadeMethodsThrowAfterTerminate( Iterable<FacadeMethod<T>> methods )
    {
        for ( final FacadeMethod<T> method : methods )
        {
            obtainEntityInTerminatedTransaction( entity ->
            {
                try
                {
                    method.call( entity );

                    fail( "Transaction was terminated, yet not exception thrown in: " + method );
                }
                catch ( TransactionTerminatedException e )
                {
                    // awesome
                }
            } );
        }
    }
}
