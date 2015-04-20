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

import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.test.OtherThreadRule;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class UniquenessConstraintCreationIT extends KernelIntegrationTest
{
    public @Rule
    TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    public @Rule
    OtherThreadRule<Void> otherThread = new OtherThreadRule<>();

    @Test
    public void shouldAbortConstraintCreationWhenDuplicatesExist() throws Exception
    {
        // given
        long node1, node2;
        int foo, name;
        {
            DataWriteOperations statement = dataWriteOperationsInNewTransaction();
            // name is not unique for Foo in the existing data

            foo = statement.labelGetOrCreateForName( "Foo" );
            name = statement.propertyKeyGetOrCreateForName( "name" );

            long node = statement.nodeCreate();
            node1 = node;
            statement.nodeAddLabel( node, foo );
            statement.nodeSetProperty( node, Property.stringProperty( name, "foo" ) );

            node = statement.nodeCreate();
            statement.nodeAddLabel( node, foo );
            node2 = node;
            statement.nodeSetProperty( node, Property.stringProperty( name, "foo" ) );
            commit();
        }

        // when
        try
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.uniquenessConstraintCreate( foo, name );

            fail( "expected exception" );
        }
        // then
        catch ( CreateConstraintFailureException ex )
        {
            assertEquals( new UniquenessConstraint( foo, name ), ex.constraint() );
            Throwable cause = ex.getCause();
            assertThat( cause, instanceOf( ConstraintVerificationFailedKernelException.class ) );
            assertEquals( asSet( new ConstraintVerificationFailedKernelException.Evidence(
                    new PreexistingIndexEntryConflictException( "foo", node1, node2 ) ) ),
                    ((ConstraintVerificationFailedKernelException) cause).evidence() );
        }
    }
}
