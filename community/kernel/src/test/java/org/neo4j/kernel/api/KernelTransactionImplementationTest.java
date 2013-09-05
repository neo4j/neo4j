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
package org.neo4j.kernel.api;

import org.junit.Test;

import org.neo4j.graphdb.TransactionFailureException;

import static org.junit.Assert.fail;

public class KernelTransactionImplementationTest
{
    @Test
    public void shouldBeAbleToRollbackTransactionThatFailsToCommit() throws Exception
    {
        // given
        KernelTransactionImplementation tx = new KernelTransactionImplementation( null, null )
        {
            @Override
            protected void doCommit()
            {
                throw new TransactionFailureException( "marked for rollback only" );
            }

            @Override
            protected void doRollback()
            {
            }

            @Override
            protected Statement newStatement()
            {
                return null;
            }
        };

        // when
        try
        {
            tx.commit();
            fail( "expected exception" );
        }
        catch ( TransactionFailureException e )
        {
            // ok
        }

        // then
        tx.rollback();
    }
}
