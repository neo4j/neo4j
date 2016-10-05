/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.properties.DefinedProperty;
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
        DataWriteOperations ops = dataWriteOperationsInNewTransaction();
        long nodeId = ops.nodeCreate();
        int labelId = ops.labelGetOrCreateForName( "Person" );
        ops.nodeAddLabel( nodeId, labelId );
        int propertyIdName = ops.propertyKeyGetOrCreateForName( "name" );
        int propertyIdAge = ops.propertyKeyGetOrCreateForName( "age" );
        ops.nodeSetProperty( nodeId, DefinedProperty.stringProperty( propertyIdName, "Emil" ) );
        commit();

        SchemaWriteOperations schemaOps = schemaWriteOperationsInNewTransaction();
        schemaOps.indexCreate( labelId, propertyIdName );
        schemaOps.uniquePropertyConstraintCreate( labelId, propertyIdAge );
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
            assertEquals( nodes.get( 0 ).getAllProperties().get( "name" ), new String( "Person" ) );
            assertEquals( nodes.get( 0 ).getAllProperties().get( "indexes" ), Arrays.asList( "name" ) );
            assertEquals( nodes.get( 0 ).getAllProperties().get( "constraints" ),
                    Arrays.asList( "CONSTRAINT ON ( person:Person ) ASSERT person.age IS UNIQUE" ) );
        }
    }

    @Test
    public void testRelationShip() throws Throwable
    {
        // Given there ar
        DataWriteOperations ops = dataWriteOperationsInNewTransaction();
        long nodeIdPerson = ops.nodeCreate();
        int labelIdPerson = ops.labelGetOrCreateForName( "Person" );
        ops.nodeAddLabel( nodeIdPerson, labelIdPerson );
        long nodeIdLocation = ops.nodeCreate();
        int labelIdLocation = ops.labelGetOrCreateForName( "Location" );
        ops.nodeAddLabel( nodeIdLocation, labelIdLocation );
        ops.relationshipCreate( ops.relationshipTypeGetOrCreateForName( "LIVES_IN" ), nodeIdPerson, nodeIdLocation );
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
