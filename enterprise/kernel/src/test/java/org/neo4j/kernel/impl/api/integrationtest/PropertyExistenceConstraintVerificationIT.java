/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.SchemaHelper;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.impl.api.OperationsFacade;
import org.neo4j.kernel.impl.newapi.Operations;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EnterpriseDatabaseRule;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.runners.Suite.SuiteClasses;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.kernel.impl.api.integrationtest.PropertyExistenceConstraintVerificationIT.NodePropertyExistenceExistenceConstrainVerificationIT;
import static org.neo4j.kernel.impl.api.integrationtest.PropertyExistenceConstraintVerificationIT.RelationshipPropertyExistenceExistenceConstrainVerificationIT;
import static org.neo4j.test.rule.concurrent.ThreadingRule.waitingWhileIn;

@RunWith( Suite.class )
@SuiteClasses( {
        NodePropertyExistenceExistenceConstrainVerificationIT.class,
        RelationshipPropertyExistenceExistenceConstrainVerificationIT.class
} )
public class PropertyExistenceConstraintVerificationIT
{
    private static final int WAIT_TIMEOUT_SECONDS = 200;

    public static class NodePropertyExistenceExistenceConstrainVerificationIT
            extends AbstractPropertyExistenceConstraintVerificationIT
    {
        @Override
        void createConstraint( DatabaseRule db, String label, String property )
        {
            SchemaHelper.createNodePropertyExistenceConstraint( db, label, property );
        }

        @Override
        String constraintCreationMethodName()
        {
            return "nodePropertyExistenceConstraintCreate";
        }

        @Override
        long createOffender( DatabaseRule db, String key )
        {
            Node node = db.createNode();
            node.addLabel( label( key ) );
            return node.getId();
        }

        @Override
        String offenderCreationMethodName()
        {
            return "nodeAddLabel"; // takes schema read lock to enforce constraints
        }

        @Override
        Class<?> getOwner()
        {
            return Operations.class;
        }

    }

    public static class RelationshipPropertyExistenceExistenceConstrainVerificationIT
            extends AbstractPropertyExistenceConstraintVerificationIT
    {
        @Override
        public void createConstraint( DatabaseRule db, String relType, String property )
        {
            SchemaHelper.createRelPropertyExistenceConstraint( db, relType, property );
        }

        @Override
        public String constraintCreationMethodName()
        {
            return "relationshipPropertyExistenceConstraintCreate";
        }

        @Override
        public long createOffender( DatabaseRule db, String key )
        {
            Node start = db.createNode();
            Node end = db.createNode();
            Relationship relationship = start.createRelationshipTo( end, withName( key ) );
            return relationship.getId();
        }

        @Override
        public String offenderCreationMethodName()
        {
            return "relationshipCreate"; // takes schema read lock to enforce constraints
        }

        @Override
        Class<?> getOwner()
        {
            return OperationsFacade.class;
        }

    }

    public abstract static class AbstractPropertyExistenceConstraintVerificationIT
    {
        private static final String KEY = "Foo";
        private static final String PROPERTY = "bar";

        @Rule
        public final DatabaseRule db = new EnterpriseDatabaseRule();
        @Rule
        public final ThreadingRule thread = new ThreadingRule();

        abstract void createConstraint( DatabaseRule db, String key, String property );

        abstract String constraintCreationMethodName();

        abstract long createOffender( DatabaseRule db, String key );

        abstract String offenderCreationMethodName();
        abstract Class<?> getOwner();

        @Test
        public void shouldFailToCreateConstraintIfSomeNodeLacksTheMandatoryProperty()
        {
            // given
            try ( Transaction tx = db.beginTx() )
            {
                createOffender( db, KEY );
                tx.success();
            }

            // when
            try
            {
                try ( Transaction tx = db.beginTx() )
                {
                    createConstraint( db, KEY, PROPERTY );
                    tx.success();
                }
                fail( "expected exception" );
            }
            // then
            catch ( QueryExecutionException e )
            {
                assertThat( e.getCause().getMessage(), startsWith( "Unable to create CONSTRAINT" ) );
            }
        }

        @Test
        public void shouldFailToCreateConstraintIfConcurrentlyCreatedEntityLacksTheMandatoryProperty() throws Exception
        {
            // when
            try
            {
                Future<Void> nodeCreation;
                try ( Transaction tx = db.beginTx() )
                {
                    createConstraint( db, KEY, PROPERTY );

                    nodeCreation = thread.executeAndAwait( createOffender(), null,
                            waitingWhileIn( getOwner(), offenderCreationMethodName() ),
                            WAIT_TIMEOUT_SECONDS, SECONDS );

                    tx.success();
                }
                nodeCreation.get();
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
                assertThat( e.getCause().getCause(),
                        instanceOf( ConstraintViolationTransactionFailureException.class ) );
            }
        }

        @Test
        public void shouldFailToCreateConstraintIfConcurrentlyCommittedEntityLacksTheMandatoryProperty()
                throws Exception
        {
            // when
            try
            {
                Future<Void> constraintCreation;
                try ( Transaction tx = db.beginTx() )
                {
                    createOffender( db, KEY );

                    constraintCreation = thread.executeAndAwait( createConstraint(), null,
                            waitingWhileIn( OperationsFacade.class, constraintCreationMethodName() ),
                            WAIT_TIMEOUT_SECONDS, SECONDS );

                    tx.success();
                }
                constraintCreation.get();
                fail( "expected exception" );
            }
            // then, we either fail to create the constraint,
            catch ( ExecutionException e )
            {
                assertThat( e.getCause(), instanceOf( QueryExecutionException.class ) );
                assertThat( e.getCause().getMessage(), startsWith( "Unable to create CONSTRAINT" ) );
            }
            // or we fail to create the offending node
            catch ( ConstraintViolationException e )
            {
                assertThat( e.getCause(), instanceOf( ConstraintViolationTransactionFailureException.class ) );
            }
        }

        private ThrowingFunction<Void,Void,RuntimeException> createOffender()
        {
            return aVoid ->
            {
                try ( Transaction tx = db.beginTx() )
                {
                    createOffender( db, KEY );
                    tx.success();
                }
                return null;
            };
        }

        private ThrowingFunction<Void,Void,RuntimeException> createConstraint()
        {
            return aVoid ->
            {
                try ( Transaction tx = db.beginTx() )
                {
                    createConstraint( db, KEY, PROPERTY );
                    tx.success();
                }
                return null;
            };
        }
    }
}
