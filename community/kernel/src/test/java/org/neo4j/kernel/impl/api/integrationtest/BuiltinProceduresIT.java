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
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.exceptions.ProcedureException;

import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureName;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.helpers.collection.IteratorUtil.asList;

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
        RawIterator<Object[],ProcedureException> stream = readOperationsInNewTransaction().procedureCallRead( procedureName( "sys", "db", "labels" ), new Object[0] );

        // Then
        assertThat( asList( stream ), contains( equalTo( new Object[]{"MyLabel"} ) ) );
    }

    @Test
    public void listPropertyKeys() throws Throwable
    {
        // Given
        DataWriteOperations ops = dataWriteOperationsInNewTransaction();
        ops.propertyKeyGetOrCreateForName( "MyProp" );
        commit();

        // When
        RawIterator<Object[],ProcedureException> stream = readOperationsInNewTransaction().procedureCallRead( procedureName( "sys", "db", "propertyKeys" ), new Object[0] );

        // Then
        assertThat( asList( stream ), contains( equalTo( new Object[]{"MyProp"} ) ) );
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
        RawIterator<Object[],ProcedureException> stream = readOperationsInNewTransaction().procedureCallRead( procedureName( "sys", "db", "relationshipTypes" ), new Object[0] );

        // Then
        assertThat( asList( stream ), contains( equalTo( new Object[]{"MyRelType"} ) ) );
    }

    @Test
    public void listProcedures() throws Throwable
    {
        // When
        RawIterator<Object[],ProcedureException> stream =
                readOperationsInNewTransaction().procedureCallRead( procedureName( "sys", "db", "procedures" ), new Object[0] );

        // Then
        assertThat( asList( stream ), contains(
                equalTo( new Object[]{"sys.db.labels", "sys.db.labels() :: (label :: STRING?)",
                        "Retrieve a list of all labels that are currently in use."} ),
                equalTo( new Object[]{"sys.db.procedures", "sys.db.procedures() :: (name :: STRING?, signature :: STRING?, description :: STRING?)",
                        "Retrieve a list of all procedures that are currently registered."} ),
                equalTo( new Object[]{"sys.db.propertyKeys", "sys.db.propertyKeys() :: (propertyKey :: STRING?)",
                        "Retrieve a list of all property keys that are currently in use."}),
                equalTo( new Object[]{"sys.db.relationshipTypes", "sys.db.relationshipTypes() :: (relationshipType :: STRING?)",
                        "Retrieve a list of all relationship types that are currently in use."})
        ));
    }
}
