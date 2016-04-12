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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.kernel.builtinprocs.ListIndexesProcedure;
import org.neo4j.server.security.auth.AuthSubject;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static junit.framework.TestCase.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;

public class BuiltinProceduresIT extends KernelIntegrationTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void listAllLabels() throws Throwable
    {
        // Given
        DataWriteOperations ops = dataWriteOperationsInNewTransaction();
        long nodeId = ops.nodeCreate();
        int labelId = ops.labelGetOrCreateForName( "MyLabel" );
        ops.nodeAddLabel( nodeId, labelId );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream = readOperationsInNewTransaction().procedureCallRead( procedureName( "db", "labels" ), new Object[0] );

        // Then
        assertThat( asList( stream ), contains( equalTo( new Object[]{"MyLabel"} ) ) );
    }

    @Test
    public void failWhenCallingListAllLabelsInDbmsMode() throws Throwable
    {
        try
        {
            // When
            RawIterator<Object[],ProcedureException> stream = dbmsOperations().procedureCallDbms( procedureName( "db", "labels" ), new Object[0],
                    AccessMode.Static.NONE );
            fail( "Should have failed." );
        }
        catch (Exception e)
        {
            // Then
            assertThat( e.getClass(), equalTo( ProcedureException.class ) );
        }
    }

    @Test
    public void listPropertyKeys() throws Throwable
    {
        // Given
        DataWriteOperations ops = dataWriteOperationsInNewTransaction();
        ops.propertyKeyGetOrCreateForName( "MyProp" );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream = readOperationsInNewTransaction().procedureCallRead( procedureName( "db", "propertyKeys" ), new Object[0] );

        // Then
        assertThat( asList( stream ), contains( equalTo( new Object[]{"MyProp"} ) ) );
    }

    @Test
    public void failWhenCallingListPropertyKeysInDbmsMode() throws Throwable
    {
        try
        {
            // When
            RawIterator<Object[],ProcedureException> stream = dbmsOperations().procedureCallDbms( procedureName( "db", "propertyKeys" ), new Object[0], AccessMode.Static.NONE );
            fail( "Should have failed." );
        }
        catch (Exception e)
        {
            // Then
            assertThat( e.getClass(), equalTo( ProcedureException.class ) );
        }
    }

    @Test
    public void listRelationshipTypes() throws Throwable
    {
        // Given
        DataWriteOperations ops = dataWriteOperationsInNewTransaction();
        int relType = ops.relationshipTypeGetOrCreateForName( "MyRelType" );
        ops.relationshipCreate( relType, ops.nodeCreate(), ops.nodeCreate() );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream = readOperationsInNewTransaction().procedureCallRead( procedureName( "db", "relationshipTypes" ), new Object[0] );

        // Then
        assertThat( asList( stream ), contains( equalTo( new Object[]{"MyRelType"} ) ) );
    }

    @Test
    public void failWhenCallingListRelationshipTypesInDbmsMode() throws Throwable
    {
        try
        {
            // When
            RawIterator<Object[],ProcedureException> stream = dbmsOperations().procedureCallDbms( procedureName( "db", "relationshipTypes" ), new Object[0], AccessMode.Static.NONE );
            fail( "Should have failed." );
        }
        catch (Exception e)
        {
            // Then
            assertThat( e.getClass(), equalTo( ProcedureException.class ) );
        }
    }

    @Test
    public void listProcedures() throws Throwable
    {
        // When
        RawIterator<Object[],ProcedureException> stream =
                readOperationsInNewTransaction().procedureCallRead( procedureName( "dbms", "procedures" ), new Object[0] );

        // Then
        assertThat( asList( stream ), containsInAnyOrder(
                equalTo( new Object[]{"db.constraints", "db.constraints() :: (description :: STRING?)"} ),
                equalTo( new Object[]{"db.indexes", "db.indexes() :: (description :: STRING?, state :: STRING?, type :: STRING?)"} ),
                equalTo( new Object[]{"db.propertyKeys", "db.propertyKeys() :: (propertyKey :: STRING?)"}),
                equalTo( new Object[]{"db.labels", "db.labels() :: (label :: STRING?)"} ),
                equalTo( new Object[]{"db.relationshipTypes", "db.relationshipTypes() :: (relationshipType :: STRING?)"}),
                equalTo( new Object[]{"dbms.procedures", "dbms.procedures() :: (name :: STRING?, signature :: STRING?)"} ),
                equalTo( new Object[]{"dbms.components", "dbms.components() :: (name :: STRING?, versions :: LIST? OF STRING?, edition :: STRING?)"} ),
                equalTo( new Object[]{"dbms.changePassword", "dbms.changePassword(password :: STRING?) :: ()"}),
                equalTo( new Object[]{"dbms.queryJmx", "dbms.queryJmx(query :: STRING?) :: (name :: STRING?, description :: STRING?, attributes :: MAP?)"})
        ));
    }

    @Test
    public void failWhenCallingListProceduresInDbmsMode() throws Throwable
    {
        try
        {
            // When
            RawIterator<Object[],ProcedureException> stream =
                    dbmsOperations()
                            .procedureCallDbms( procedureName( "sys", "procedures" ), new Object[0],
                                    AccessMode.Static.NONE );
            assertThat( "This should never get here", 1 == 2 );
        }
        catch (Exception e)
        {
            // Then
            assertThat( e.getClass(), equalTo( ProcedureException.class ) );
        }
    }

    @Test
    public void callChangePasswordWithAccessModeInDbmsMode() throws Throwable
    {
        // Given
        Object[] inputArray = new Object[1];
        inputArray[0] = "newPassword";
        AuthSubject authSubject = mock( AuthSubject.class );

        // When
        RawIterator < Object[],ProcedureException> stream = dbmsOperations()
                .procedureCallDbms( procedureName( "dbms", "changePassword" ), inputArray, authSubject );

        // Then
        verify( authSubject ).setPassword( (String) inputArray[0] );
        assertThat( asList( stream ), emptyIterable() );
    }

    @Test
    public void shouldFailWhenChangePasswordWithStaticAccessModeInDbmsMode() throws Throwable
    {
        try
        {
            // Given
            Object[] inputArray = new Object[1];
            inputArray[0] = "newPassword";

            // When
            RawIterator<Object[],ProcedureException> stream = dbmsOperations()
                    .procedureCallDbms( procedureName( "dbms", "changePassword" ), inputArray,
                            AccessMode.Static.NONE );
            fail( "Should have failed." );
        }
        catch ( Exception e )
        {
            // Then
            assertThat( e.getClass(), equalTo( AuthorizationViolationException.class ) );
        }
    }

    @Test
    public void listAllIndexes() throws Throwable
    {
        // Given
        SchemaWriteOperations ops = schemaWriteOperationsInNewTransaction();
        int labelId1 = ops.labelGetOrCreateForName( "Person" );
        int labelId2 = ops.labelGetOrCreateForName( "Age" );
        int propertyKeyId = ops.propertyKeyGetOrCreateForName( "foo" );
        ops.indexCreate( labelId1, propertyKeyId );
        ops.uniquePropertyConstraintCreate( labelId2, propertyKeyId );
        commit();

        //let indexes come online
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexOnline( db.schema().getIndexes().iterator().next(), 20, SECONDS );
            tx.success();
        }

        // When
        RawIterator<Object[],ProcedureException> stream =
                readOperationsInNewTransaction().procedureCallRead( procedureName( "db", "indexes" ), new Object[0] );

        // Then
        assertThat( stream.next(), equalTo( new Object[]{"INDEX ON :Age(foo)", "online",
                ListIndexesProcedure.IndexType.NODE_UNIQUE_PROPERTY.typeName()} ) );
        assertThat( stream.next(), equalTo( new Object[]{"INDEX ON :Person(foo)", "online",
                ListIndexesProcedure.IndexType.NODE_LABEL_PROPERTY.typeName()} ) );
    }

    @Test
    public void shouldFalilistAllIndexesnDbmsMode() throws Throwable
    {
        try
        {
            // When
            RawIterator<Object[],ProcedureException> stream = dbmsOperations().procedureCallDbms( procedureName( "db", "indexes" ), new Object[0],
                    AccessMode.Static.NONE );
            fail( "Should have failed." );
        }
        catch (Exception e)
        {
            // Then
            assertThat( e.getClass(), equalTo( ProcedureException.class ) );
        }
    }

    @Test
    public void listAllComponents() throws Throwable
    {
        // Given a running database

        // When
        RawIterator<Object[],ProcedureException> stream =
                readOperationsInNewTransaction().procedureCallRead( procedureName( "dbms", "components" ), new Object[0] );

        // Then
        assertThat( asList( stream ), contains( equalTo( new Object[]{"Neo4j Kernel", singletonList("dev"), "community"} ) ) );
    }
}
