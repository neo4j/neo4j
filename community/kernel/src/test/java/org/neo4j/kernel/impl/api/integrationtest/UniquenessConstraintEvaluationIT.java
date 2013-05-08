/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.kernel.impl.api.constraints.ConstraintVerificationFailedKernelException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicLabel.label;

@Ignore("2013-05-03: Does not work yet - should work REALLY soon. (so soon that this should never be merged)")
public class UniquenessConstraintEvaluationIT extends KernelIntegrationTest
{
    @Test
    public void shouldAbortConstraintCreationWhenDuplicatesExist() throws Exception
    {
        // given
        newTransaction();
        // name is not unique for Foo in the existing data
        db.createNode( label( "Foo" ) ).setProperty( "name", "foo" );
        db.createNode( label( "Foo" ) ).setProperty( "name", "foo" );
        long foo = statement.getLabelId( "Foo" );
        long name = statement.getPropertyKeyId( "name" );
        commit();

        newTransaction();

        // when
        try
        {
            statement.addUniquenessConstraint( foo, name );

            fail( "expected exception" );
        }
        // then
        catch ( Exception ex )
        {
            assertThat( ex, instanceOf( ConstraintVerificationFailedKernelException.class ) );
            ex.printStackTrace();
            assertNotNull( ex );
        }
    }

    @Test
    public void shouldFailOnCommitIfConstraintIsBrokenAfterConstraintAdded() throws Exception
    {
        // given
        newTransaction();
        db.createNode( label( "Foo" ) ).setProperty( "name", "foo" );
        long foo = statement.getLabelId( "Foo" );
        long name = statement.getPropertyKeyId( "name" );
        commit();

        newTransaction();
        statement.addUniquenessConstraint( foo, name );
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit( new Runnable()
        {
            @Override
            public void run()
            {
                newTransaction();
                db.createNode( label( "Foo" ) ).setProperty( "name", "foo" );
                commit();
            }
        } ).get();
        executor.shutdown();

        // when
        try
        {
            commit();

            fail( "expected exception" );
        }
        // then
        catch ( Exception ex ) // TODO: narrow this catch clause
        {
            ex.printStackTrace();
            assertNotNull( ex );
        }
    }

    @Test
    public void shouldEnforceUniquenessConstraint() throws Exception
    {
        // given
        newTransaction();
        db.createNode( label( "Foo" ) ).setProperty( "name", "foo" );
        long foo = statement.getLabelId( "Foo" );
        long name = statement.getPropertyKeyId( "name" );
        commit();
        newTransaction();
        statement.addUniquenessConstraint( foo, name );
        commit();

        newTransaction();

        // when
        try
        {
            db.createNode( label( "Foo" ) ).setProperty( "name", "foo" );

            fail( "expected exception" );
        }
        // then
        catch ( Exception ex ) // TODO: narrow this catch clause
        {
            ex.printStackTrace();
            assertNotNull( ex );
        }
    }
}
