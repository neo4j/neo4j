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

import org.neo4j.collection.RawIterator;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_LIST;

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
        RawIterator<AnyValue[],ProcedureException> stream =
               procs.procedureCallRead( procs.procedureGet( procedureName( "db", "schema", "visualization" ) ).id(), new AnyValue[0] );

        // Then
        assertThat( asList( stream ), contains( equalTo( new AnyValue[]{EMPTY_LIST, EMPTY_LIST} ) ) );
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
        schemaOps.indexCreate( SchemaDescriptorFactory.forLabel( labelId, propertyIdName ) );
        schemaOps.uniquePropertyConstraintCreate( SchemaDescriptorFactory.forLabel( labelId, propertyIdAge ) );
        commit();

        // When
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet( procedureName( "db", "schema", "visualization" ) ).id(),
                        new AnyValue[0] );

        // Then
        while ( stream.hasNext() )
        {
            AnyValue[] next = stream.next();
            assertEquals( 2, next.length );
            ListValue nodes = (ListValue) next[0];
            assertEquals( 1, nodes.size() );
            NodeValue node = (NodeValue) nodes.value( 0 );
            assertThat( node.labels(), equalTo( Values.stringArray( "Person" ) )  );
            assertEquals( stringValue( "Person") , node.properties().get( "name" ) );
            assertEquals( VirtualValues.list( stringValue( "name" ) ),node.properties().get( "indexes" ) );
            assertEquals( VirtualValues.list( stringValue( "CONSTRAINT ON ( person:Person ) ASSERT person.age IS UNIQUE" )),
                    node.properties().get( "constraints" ) );
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
        RawIterator<AnyValue[],ProcedureException> stream =
                procs().procedureCallRead( procs().procedureGet(  procedureName( "db", "schema", "visualization" ) ).id(),
                        new AnyValue[0] );

        // Then
        while ( stream.hasNext() )
        {
            AnyValue[] next = stream.next();
            assertEquals( 2, next.length );
            ListValue relationships = (ListValue) next[1];
            assertEquals( 1, relationships.size() );
            RelationshipValue relationship = (RelationshipValue) relationships.value( 0 );
            assertEquals( "LIVES_IN", relationship.type().stringValue() );
            assertThat(relationship.startNode().labels(),
                    equalTo( Values.stringArray(  "Person" ) ) );
            assertThat(relationship.endNode().labels(),
                    equalTo( Values.stringArray(  "Location" ) ) );
        }
        commit();
    }
}
