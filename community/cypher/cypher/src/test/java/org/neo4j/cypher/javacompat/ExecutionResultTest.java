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
package org.neo4j.cypher.javacompat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import org.neo4j.cypher.ArithmeticException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

public class ExecutionResultTest
{
    @Rule
    public final ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();
    private ExecutionEngine engine;

    @Before
    public void initializeExecutionEngine() throws Exception
    {
        engine = new ExecutionEngine( db );
    }

    //TODO this test is not valid for compiled runtime as the transaction will be closed when the iterator was created
    @Test
    public void shouldCloseTransactionsWhenIteratingResults() throws Exception
    {
        // Given an execution result that has been started but not exhausted
        createNode();
        createNode();
        ExecutionResult executionResult = engine.execute( "CYPHER runtime=interpreted MATCH (n) RETURN n" );
        ResourceIterator<Map<String,Object>> resultIterator = executionResult.iterator();
        resultIterator.next();
        assertThat( activeTransaction(), is( notNullValue() ) );

        // When
        resultIterator.close();

        // Then
        assertThat( activeTransaction(), is( nullValue() ) );
    }

    //TODO this test is not valid for compiled runtime as the transaction will be closed when the iterator was created
    @Test
    public void shouldCloseTransactionsWhenIteratingOverSingleColumn() throws Exception
    {
        // Given an execution result that has been started but not exhausted
        createNode();
        createNode();
        ExecutionResult executionResult = engine.execute( "CYPHER runtime=interpreted MATCH (n) RETURN n" );
        ResourceIterator<Node> resultIterator = executionResult.columnAs( "n" );
        resultIterator.next();
        assertThat( activeTransaction(), is( notNullValue() ) );

        // When
        resultIterator.close();

        // Then
        assertThat( activeTransaction(), is( nullValue() ) );
    }

    @Test( expected = ArithmeticException.class )
    public void shouldThrowAppropriateException() throws Exception
    {
        engine.execute( "RETURN rand()/0" ).iterator().next();
    }

    @Test( expected = ArithmeticException.class )
    public void shouldThrowAppropriateExceptionAlsoWhenVisiting() throws Exception
    {
        engine.execute( "RETURN rand()/0" ).accept( new Result.ResultVisitor()
        {
            @Override
            public boolean visit( Result.ResultRow row )
            {
                return true;
            }
        } );
    }


    private void createNode()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
    }

    private org.neo4j.kernel.TopLevelTransaction activeTransaction()
    {
        ThreadToStatementContextBridge bridge = db.getDependencyResolver().resolveDependency(
                ThreadToStatementContextBridge.class );
        return bridge.getTopLevelTransactionBoundToThisThread( false );
    }

}
