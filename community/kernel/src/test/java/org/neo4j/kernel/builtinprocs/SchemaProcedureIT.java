/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.builtinprocs;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;

import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;

public class SchemaProcedureIT extends KernelIntegrationTest
{

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testEmptyGraph() throws Throwable
    {
        // Given the database is empty

        // When
        Procedures procs = procs();
        RawIterator<Object[],ProcedureException> stream =
               procs.procedureCallRead( procs.procedureGet( procedureName( "db", "schema" ) ).id(), new Object[0] );

        // Then
        assertThat( asList( stream ), contains( equalTo( new Object[]{new ArrayList<>(), new ArrayList<>()} ) ) );
        commit();
    }

    @Test
    public void testLabelIndex() throws Throwable
    {
        // Given there is label with index and a constraint
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeId = transaction.dataWrite().nodeCreate();
        int labelId = transaction.tokenWrite().labelGetOrCreateForName( "Person" );
        transaction.dataWrite().nodeAddLabel( nodeId, labelId );
        int propertyIdName = transaction.tokenWrite().propertyKeyGetOrCreateForName( "name" );
        int propertyIdAge = transaction.tokenWrite().propertyKeyGetOrCreateForName( "age" );
        transaction.dataWrite()
                .nodeSetProperty( nodeId, propertyIdName, Values.of( "Emil" ) );
        commit();

        SchemaWrite schemaOps = schemaWriteInNewTransaction();
        schemaOps.indexCreate( SchemaDescriptorFactory.forLabel( labelId, propertyIdName ), null );
        schemaOps.uniquePropertyConstraintCreate( SchemaDescriptorFactory.forLabel( labelId, propertyIdAge ) );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "schema" ) ).id(),
                        new Object[0] );

        // Then
        while ( stream.hasNext() )
        {
            Object[] next = stream.next();
            assertTrue( next.length == 2 );
            ArrayList<Node> nodes = (ArrayList<Node>) next[0];
            assertTrue( nodes.size() == 1 );
            assertThat( nodes.get( 0 ).getLabels(), contains( equalTo( Label.label( "Person" ) ) ) );
            assertEquals( "Person", nodes.get( 0 ).getAllProperties().get( "name" ) );
            assertEquals( Collections.singletonList( "name" ), nodes.get( 0 ).getAllProperties().get( "indexes" ) );
            assertEquals( Collections.singletonList( "CONSTRAINT ON ( person:Person ) ASSERT person.age IS UNIQUE" ),
                    nodes.get( 0 ).getAllProperties().get( "constraints" ) );
        }
        commit();
    }

    @Test
    public void testRelationShip() throws Throwable
    {
        // Given there ar
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        long nodeIdPerson = transaction.dataWrite().nodeCreate();
        int labelIdPerson = transaction.tokenWrite().labelGetOrCreateForName( "Person" );
        transaction.dataWrite().nodeAddLabel( nodeIdPerson, labelIdPerson );
        long nodeIdLocation = transaction.dataWrite().nodeCreate();
        int labelIdLocation = transaction.tokenWrite().labelGetOrCreateForName( "Location" );
        transaction.dataWrite().nodeAddLabel( nodeIdLocation, labelIdLocation );
        int relationshipTypeId = transaction.tokenWrite().relationshipTypeGetOrCreateForName( "LIVES_IN" );
        transaction.dataWrite().relationshipCreate(  nodeIdPerson, relationshipTypeId, nodeIdLocation );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet(  procedureName( "db", "schema" ) ).id(),
                        new Object[0] );

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
        commit();
    }
}
