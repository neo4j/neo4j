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
package org.neo4j.kernel.builtinprocs;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.values.storable.TextValue;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SchemaIndex.NATIVE10;
import static org.neo4j.values.storable.Values.stringValue;

@RunWith( Parameterized.class )
public class EnterpriseCreateIndexProcedureIT extends CreateIndexProcedureIT
{
    @Parameterized.Parameters( name = "{1}")
    public static Collection<Object[]> parameters()
    {
        return Arrays.asList(
                new Object[]{ false, "createUniquePropertyConstraint", "uniqueness constraint online" },
                new Object[]{ true, "createNodeKey", "node key constraint online" }
        );
    }

    @Parameterized.Parameter()
    public static boolean existenceConstraint;

    @Parameterized.Parameter( 1 )
    public static String indexProcedureName;

    @Parameterized.Parameter( 2 )
    public static String expectedSuccessfulCreationStatus;

    @Test
    public void throwOnUniquenessViolation() throws Exception
    {
        testThrowOnUniquenessViolation( "MyLabel", "oneKey" );
    }

    @Test
    public void throwOnUniquenessViolationComposite() throws Exception
    {
        testThrowOnUniquenessViolation( "MyLabel", "oneKey", "anotherKey" );
    }

    @Test
    public void throwOnNonUniqueStore() throws Exception
    {
        // given
        String label = "SomeLabel";
        String[] properties = new String[]{"key1", "key2"};
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        int labelId = transaction.tokenWrite().labelGetOrCreateForName( label );
        int[] propertyKeyIds = createProperties( transaction, properties );
        TextValue value = stringValue( "some value" );
        createNodeWithPropertiesAndLabel( transaction, labelId, propertyKeyIds, value );
        createNodeWithPropertiesAndLabel( transaction, labelId, propertyKeyIds, value );
        commit();

        // when
        try
        {
            createConstraint( label, properties );
            fail( "Should have failed" );
        }
        catch ( ProcedureException e )
        {
            // then
            // good
            assertTrue( "correct cause", e.getCause() instanceof UniquePropertyValueValidationException );
        }
    }

    @Test
    public void throwOnExistenceViolation() throws Exception
    {
        assumeThat( "Only relevant for existence constraints", existenceConstraint, is( true ) );

        // given
        String label = "label";
        String prop = "key";
        createConstraint( label, prop );

        // when
        try
        {
            try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
            {
                db.createNode( Label.label( label ) );
                tx.success();
            }
            fail( "Should have failed" );
        }
        catch ( ConstraintViolationException e )
        {
            // then
            // good
        }
    }

    @SuppressWarnings( "SameParameterValue" )
    private void testThrowOnUniquenessViolation( String label, String... properties ) throws Exception
    {
        // given
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        int labelId = transaction.tokenWrite().labelGetOrCreateForName( label );
        int[] propertyKeyIds = createProperties( transaction, properties );
        TextValue value = stringValue( "some value" );
        createNodeWithPropertiesAndLabel( transaction, labelId, propertyKeyIds, value );
        commit();

        createConstraint( label, properties );

        // when
        try
        {
            transaction = newTransaction( AnonymousContext.write() );
            createNodeWithPropertiesAndLabel( transaction, labelId, propertyKeyIds, value );
            fail( "Should have failed" );
        }
        catch ( UniquePropertyValueValidationException e )
        {
            // then
            // ok
        }
    }

    private void createConstraint( String label, String... properties ) throws TransactionFailureException, ProcedureException
    {
        newTransaction( AnonymousContext.full() );
        String pattern = indexPattern( label, properties );
        String specifiedProvider = NATIVE10.providerName();
        callIndexProcedure( pattern, specifiedProvider );
        commit();
    }

    @Override
    protected String indexProcedureName()
    {
        return indexProcedureName;
    }

    @Override
    protected String expectedSuccessfulCreationStatus()
    {
        return expectedSuccessfulCreationStatus;
    }

    @Override
    protected boolean expectUnique()
    {
        return true;
    }

    @Override
    protected TestGraphDatabaseFactory createGraphDatabaseFactory()
    {
        return new TestEnterpriseGraphDatabaseFactory();
    }
}
