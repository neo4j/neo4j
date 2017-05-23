/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.coreapi;

import org.junit.Test;

import java.util.function.LongFunction;
import java.util.function.Supplier;

import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.function.Suppliers.singleton;

public class StandardRelationshipActionsTest
{
    @Test
    public void closeStatementOnRetrievingRelationshipTypeById() throws Exception
    {
        Statement statement = mock( Statement.class );
        CountingSupplier statementSupplier = new CountingSupplier( statement );
        StandardRelationshipActions relationshipActions = setupRelationshipActions( statement, statementSupplier );

        RelationshipType relationshipType = relationshipActions.getRelationshipTypeById( 4 );

        assertEquals( RelationshipType.withName("testType"), relationshipType );
        assertEquals( "Statement should be acquired only once.", 1, statementSupplier.getInvocationCounter() );
        verify( statement ).close();
    }

    private StandardRelationshipActions setupRelationshipActions( Statement statement,
            CountingSupplier statementSupplier ) throws RelationshipTypeIdNotFoundKernelException
    {
        ReadOperations readOperations = getReadOperations( statement );
        when( statement.readOperations() ).thenReturn( readOperations );
        KernelTransaction kernelTransaction = mock( KernelTransaction.class );
        GraphDatabaseService databaseService = mock( GraphDatabaseService.class );
        LongFunction<Node> longFunction = value -> null;
        return new StandardRelationshipActions( statementSupplier,
                singleton( kernelTransaction ), ThrowingAction.noop(), longFunction, databaseService );
    }

    private ReadOperations getReadOperations( Statement statement ) throws RelationshipTypeIdNotFoundKernelException
    {
        ReadOperations readOperations = mock( ReadOperations.class );
        when( readOperations.relationshipTypeGetName( 4 ) ).thenReturn( "testType" );
        return readOperations;
    }

    private static class CountingSupplier implements Supplier<Statement>
    {
        private final Statement statement;
        private int invocationCounter;

        CountingSupplier( Statement statement )
        {
            this.statement = statement;
        }

        @Override
        public Statement get()
        {
            invocationCounter++;
            return statement;
        }

        int getInvocationCounter()
        {
            return invocationCounter;
        }
    }
}
