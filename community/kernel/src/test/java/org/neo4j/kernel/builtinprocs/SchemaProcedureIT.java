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
package org.neo4j.kernel.builtinprocs;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;

public class SchemaProcedureIT extends KernelIntegrationTest
{

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testEmptyGraph() throws Throwable
    {
        // Given the database is empty

        // When
        RawIterator<Object[],ProcedureException> stream =
                procedureCallOpsInNewTx().procedureCallRead( procedureName( "db", "schema" ), new Object[0] );

        // Then
        assertThat( asList( stream ), contains( equalTo( new Object[]{new ArrayList<>(), new ArrayList<>()} ) ) );
    }

    @Test
    public void testLabelIndex() throws Throwable
    {
        // Given there is label with index and a constraint
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        long nodeId = statement.dataWriteOperations().nodeCreate();
        int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( "Person" );
        statement.dataWriteOperations().nodeAddLabel( nodeId, labelId );
        int propertyIdName = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "name" );
        int propertyIdAge = statement.tokenWriteOperations().propertyKeyGetOrCreateForName( "age" );
        statement.dataWriteOperations()
                .nodeSetProperty( nodeId, DefinedProperty.stringProperty( propertyIdName, "Emil" ) );
        commit();

        SchemaWriteOperations schemaOps = schemaWriteOperationsInNewTransaction();
        schemaOps.indexCreate( SchemaDescriptorFactory.forLabel( labelId, propertyIdName ) );
        schemaOps.uniquePropertyConstraintCreate( SchemaDescriptorFactory.forLabel( labelId, propertyIdAge ) );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream =
                procedureCallOpsInNewTx().procedureCallRead( procedureName( "db", "schema" ), new Object[0] );

        // Then
        while ( stream.hasNext() )
        {
            Object[] next = stream.next();
            assertTrue( next.length == 2 );
            ArrayList<Node> nodes = (ArrayList<Node>) next[0];
            assertTrue( nodes.size() == 1 );
            assertThat( nodes.get( 0 ).getLabels(), contains( equalTo( Label.label( "Person" ) ) ) );
            assertEquals( new String( "Person" ), nodes.get( 0 ).getAllProperties().get( "name" ) );
            assertEquals( Arrays.asList( "name" ), nodes.get( 0 ).getAllProperties().get( "indexes" ) );
            assertEquals( Arrays.asList( "CONSTRAINT ON ( person:Person ) ASSERT person.age IS UNIQUE" ),
                    nodes.get( 0 ).getAllProperties().get( "constraints" ) );
        }
    }

    @Test
    public void testRelationShip() throws Throwable
    {
        // Given there ar
        Statement statement = statementInNewTransaction( AnonymousContext.writeToken() );
        long nodeIdPerson = statement.dataWriteOperations().nodeCreate();
        int labelIdPerson = statement.tokenWriteOperations().labelGetOrCreateForName( "Person" );
        statement.dataWriteOperations().nodeAddLabel( nodeIdPerson, labelIdPerson );
        long nodeIdLocation = statement.dataWriteOperations().nodeCreate();
        int labelIdLocation = statement.tokenWriteOperations().labelGetOrCreateForName( "Location" );
        statement.dataWriteOperations().nodeAddLabel( nodeIdLocation, labelIdLocation );
        int relationshipTypeId = statement.tokenWriteOperations().relationshipTypeGetOrCreateForName( "LIVES_IN" );
        statement.dataWriteOperations().relationshipCreate( relationshipTypeId, nodeIdPerson, nodeIdLocation );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream =
                procedureCallOpsInNewTx().procedureCallRead( procedureName( "db", "schema" ), new Object[0] );

        // Then
        while ( stream.hasNext() )
        {
            Object[] next = stream.next();
            assertTrue( next.length == 2 );
            LinkedList<Relationship> relationships = (LinkedList<Relationship>) next[1];
            assertTrue( relationships.size() == 1 );
            assertEquals( "LIVES_IN", relationships.get( 0 ).getType().name() );
            assertThat( relationships.get( 0 ).getStartNode().getLabels(),
                    contains( equalTo( Label.label( "Person" ) ) ) );
            assertThat( relationships.get( 0 ).getEndNode().getLabels(),
                    contains( equalTo( Label.label( "Location" ) ) ) );

        }
    }
}
