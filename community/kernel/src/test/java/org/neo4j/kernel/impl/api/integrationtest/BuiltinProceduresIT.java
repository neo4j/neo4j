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

import java.util.stream.Stream;

import org.neo4j.kernel.api.DataWriteOperations;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.proc.ProcedureSignature.procedureName;

public class BuiltinProceduresIT extends KernelIntegrationTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void listAllLabels() throws Throwable
    {
        // Given
        DataWriteOperations ops = dataWriteOperationsInNewTransaction();
        ops.labelGetOrCreateForName( "MyLabel" );
        commit();

        // When
        Stream<Object[]> stream = readOperationsInNewTransaction().procedureCallRead( procedureName( "sys", "db", "labels" ), new Object[0] );

        // Then
        assertThat( stream.collect( toList() ), contains( equalTo( new Object[]{"MyLabel"} ) ) );
    }

    @Test
    public void listPropertyKeys() throws Throwable
    {
        // Given
        DataWriteOperations ops = dataWriteOperationsInNewTransaction();
        ops.propertyKeyGetOrCreateForName( "MyProp" );
        commit();

        // When
        Stream<Object[]> stream = readOperationsInNewTransaction().procedureCallRead( procedureName( "sys", "db", "propertyKeys" ), new Object[0] );

        // Then
        assertThat( stream.collect( toList() ), contains( equalTo( new Object[]{"MyProp"} ) ) );
    }

    @Test
    public void listRelationshipTypes() throws Throwable
    {
        // Given
        DataWriteOperations ops = dataWriteOperationsInNewTransaction();
        ops.relationshipTypeGetOrCreateForName( "MyRelType" );
        commit();

        // When
        Stream<Object[]> stream = readOperationsInNewTransaction().procedureCallRead( procedureName( "sys", "db", "relationshipTypes" ), new Object[0] );

        // Then
        assertThat( stream.collect( toList() ), contains( equalTo( new Object[]{"MyRelType"} ) ) );
    }
}
