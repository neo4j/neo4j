/*
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

package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.TokenNameLookup;
import org.neo4j.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.impl.api.OperationsFacade;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.ThreadingRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.test.DatabaseFunctions.addLabel;
import static org.neo4j.test.DatabaseFunctions.createNode;
import static org.neo4j.test.DatabaseFunctions.mandatoryPropertyConstraint;
import static org.neo4j.test.ThreadingRule.waitingWhileIn;

public class MandatoryPropertyConstraintVerificationIT
{
    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();
    @Rule
    public final ThreadingRule thread = new ThreadingRule();

    @Test
    public void shouldFailToCreateConstraintIfSomeNodeLacksTheMandatoryProperty() throws Exception
    {
        // given
        long nodeId;
        try ( Transaction tx = db.beginTx() )
        {
            nodeId = db.createNode( label( "Foo" ) ).getId();

            tx.success();
        }

        // when
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().constraintFor( label( "Foo" ) ).assertPropertyExists( "bar" ).create();

                tx.success();
            }
            fail( "expected exception" );
        }
        // then
        catch ( ConstraintViolationException e )
        {
            Throwable cause = e.getCause();
            assertThat( cause, instanceOf( CreateConstraintFailureException.class ) );

            Throwable rootCause = cause.getCause();
            assertThat( rootCause, instanceOf( ConstraintVerificationFailedKernelException.class ) );
            assertThat( userMessageOf( (KernelException) rootCause ), containsString( "Node(" + nodeId + ")" ) );
        }
    }

    @Test
    public void shouldFailToCreateConstraintIfConcurrentlyCreatedNodeLacksTheMandatoryProperty() throws Exception
    {
        // when
        try
        {
            Future<Node> node;
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().constraintFor( label( "Foo" ) ).assertPropertyExists( "bar" ).create();

                node = thread.executeAndAwait( db.tx( createNode().then( addLabel( label( "Foo" ) ) ) ),
                        db.getGraphDatabaseService(),
                        waitingWhileIn( OperationsFacade.class, "nodeAddLabel" ), 5, SECONDS );

                tx.success();
            }
            node.get();
            fail( "expected exception" );
        }
        // then, we either fail to create the constraint,
        catch ( ConstraintViolationException e )
        {
            assertThat( e.getCause(), instanceOf( CreateConstraintFailureException.class ) );
        }
        // or we fail to create the offending node
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), instanceOf( ConstraintViolationException.class ) );
            assertThat( e.getCause().getCause(), instanceOf( ConstraintViolationTransactionFailureException.class ) );
        }
    }

    @Test
    public void shouldFailToCreateConstraintIfConcurrentlyCommittedNodeLacksTheMandatoryProperty() throws Exception
    {
        // when
        try
        {
            Future<Void> constraint;
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode( label( "Foo" ) );

                constraint = thread.executeAndAwait( db.tx( mandatoryPropertyConstraint( label( "Foo" ), "bar" ) ),
                        db.getGraphDatabaseService(),
                        waitingWhileIn( OperationsFacade.class, "mandatoryPropertyConstraintCreate" ), 5, SECONDS );

                tx.success();
            }
            constraint.get();
            fail( "expected exception" );
        }
        // then, we either fail to create the constraint,
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), instanceOf( ConstraintViolationException.class ) );
            assertThat( e.getCause().getCause(), instanceOf( CreateConstraintFailureException.class ) );
        }
        // or we fail to create the offending node
        catch ( ConstraintViolationException e )
        {
            assertThat( e.getCause(), instanceOf( ConstraintViolationTransactionFailureException.class ) );
        }
    }

    private String userMessageOf( KernelException exception )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            DependencyResolver dependencyResolver = db.getGraphDatabaseAPI().getDependencyResolver();
            Statement statement = dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class ).get();
            TokenNameLookup tokenNameLookup = new StatementTokenNameLookup( statement.readOperations() );
            return exception.getUserMessage( tokenNameLookup );
        }
    }
}
