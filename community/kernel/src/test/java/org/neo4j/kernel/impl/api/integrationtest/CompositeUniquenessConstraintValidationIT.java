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
package org.neo4j.kernel.impl.api.integrationtest;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Session;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.fail;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.test.assertion.Assert.assertException;

@RunWith( Parameterized.class )
public class CompositeUniquenessConstraintValidationIT
{
    @ClassRule
    public static ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    @Rule
    public final TestName testName = new TestName();
    private final int numberOfProps;
    private final Object[] aValues;
    private final Object[] bValues;

    @Parameterized.Parameters( name = "{index}: {0}" )
    public static Iterable<TestParams> parameterValues()
    {
        return Arrays.asList(
                param( values( 10 ), values( 10d ) ),
                param( values( 10, 20 ), values( 10, 20 ) ),
                param( values( 10L, 20L ), values( 10, 20 ) ),
                param( values( 10, 20 ), values( 10L, 20L ) ),
                param( values( 10, 20 ), values( 10.0, 20.0 ) ),
                param( values( 10, 20 ), values( 10.0, 20.0 ) ),
                param( values( new int[]{1, 2}, "v2" ), values( new int[]{1, 2}, "v2" ) ),
                param( values( "a", "b", "c" ), values( "a", "b", "c" ) ),
                param( values( 285414114323346805L ), values( 285414114323346805L ) ),
                param( values( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ), values( 1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d ) )
        );
    }

    private static TestParams param( Object[] l, Object[] r )
    {
        return new TestParams( l, r );
    }

    private static Object[] values( Object... values )
    {
        return values;
    }

    private static final int label = 1;

    public CompositeUniquenessConstraintValidationIT( TestParams params )
    {
        assert params.lhs.length == params.rhs.length;
        aValues = params.lhs;
        bValues = params.rhs;
        numberOfProps = aValues.length;
    }

    private Transaction transaction;
    private GraphDatabaseAPI graphDatabaseAPI;
    protected Kernel kernel;
    protected Session session;

    @Before
    public void setup() throws Exception
    {
        graphDatabaseAPI = dbRule.getGraphDatabaseAPI();
        kernel = graphDatabaseAPI.getDependencyResolver().resolveDependency( Kernel.class );
        session = kernel.beginSession( LoginContext.AUTH_DISABLED );

        newTransaction();
        transaction.schemaWrite().uniquePropertyConstraintCreate( forLabel( label, propertyIds() ) );
        commit();
    }

    @After
    public void clean() throws Exception
    {
        if ( transaction != null )
        {
            transaction.close();
        }

        newTransaction();
        transaction.schemaWrite()
                .constraintDrop( ConstraintDescriptorFactory.uniqueForLabel( label, propertyIds() ) );
        commit();

        try ( Transaction tx = session.beginTransaction( Transaction.Type.implicit );
              NodeCursor node = tx.cursors().allocateNodeCursor() )
        {
            tx.dataRead().allNodesScan( node );
            while ( node.next() )
            {
                tx.dataWrite().nodeDelete( node.nodeReference() );
            }
            tx.success();
        }
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_DeleteNode() throws Exception
    {
        // given
        long node = createNodeWithLabelAndProps( label, aValues );

        // when
        newTransaction();
        transaction.dataWrite().nodeDelete( node );
        long newNode = createLabeledNode( label );
        setProperties( newNode, aValues );

        // then does not fail
        commit();
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_RemoveLabel() throws Exception
    {
        // given
        long node = createNodeWithLabelAndProps( label, aValues );

        // when
        newTransaction();
        transaction.dataWrite().nodeRemoveLabel( node, label );
        long newNode = createLabeledNode( label );
        setProperties( newNode, aValues );

        // then does not fail
        commit();
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_RemoveProperty() throws Exception
    {
        // given
        long node = createNodeWithLabelAndProps( label, aValues );

        // when
        newTransaction();
        transaction.dataWrite().nodeRemoveProperty( node, 0 );
        long newNode = createLabeledNode( label );
        setProperties( newNode, aValues );

        // then does not fail
        commit();
    }

    @Test
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_ChangeProperty() throws Exception
    {
        // given
        long node = createNodeWithLabelAndProps( label, aValues );

        // when
        newTransaction();
        transaction.dataWrite().nodeSetProperty( node, 0, Values.of( "Alive!" ) );
        long newNode = createLabeledNode( label );
        setProperties( newNode, aValues );

        // then does not fail
        commit();
    }

    @Test
    public void shouldPreventConflictingDataInTx() throws Throwable
    {
        // Given

        // When
        newTransaction();
        long n1 = createLabeledNode( label );
        long n2 = createLabeledNode( label );
        setProperties( n1, aValues );
        int lastPropertyOffset = numberOfProps - 1;
        for ( int prop = 0; prop < lastPropertyOffset; prop++ )
        {
            setProperty( n2, prop, aValues[prop] ); // still ok
        }

        assertException( () ->
        {
            setProperty( n2, lastPropertyOffset, aValues[lastPropertyOffset] ); // boom!

        }, UniquePropertyValueValidationException.class );

        // Then should fail
        commit();
    }

    @Test
    public void shouldEnforceOnSetProperty() throws Exception
    {
        // given
        createNodeWithLabelAndProps( label, this.aValues );

        // when
        newTransaction();
        long node = createLabeledNode( label );

        int lastPropertyOffset = numberOfProps - 1;
        for ( int prop = 0; prop < lastPropertyOffset; prop++ )
        {
            setProperty( node, prop, aValues[prop] ); // still ok
        }

        assertException( () ->
        {
            setProperty( node, lastPropertyOffset, aValues[lastPropertyOffset] ); // boom!

        }, UniquePropertyValueValidationException.class );
        commit();
    }

    @Test
    public void shouldEnforceOnSetLabel() throws Exception
    {
        // given
        createNodeWithLabelAndProps( label, this.aValues );

        // when
        newTransaction();
        long node = createNode();
        setProperties( node, bValues ); // ok because no label is set

        assertException( () ->
        {
            addLabel( node, label ); // boom!

        }, UniquePropertyValueValidationException.class );
        commit();
    }

    @Test
    public void shouldEnforceOnSetPropertyInTx() throws Exception
    {
        // when
        newTransaction();
        long aNode = createLabeledNode( label );
        setProperties( aNode, aValues );

        long nodeB = createLabeledNode( label );
        int lastPropertyOffset = numberOfProps - 1;
        for ( int prop = 0; prop < lastPropertyOffset; prop++ )
        {
            setProperty( nodeB, prop, bValues[prop] ); // still ok
        }

        assertException( () ->
        {
            setProperty( nodeB, lastPropertyOffset, bValues[lastPropertyOffset] ); // boom!
        }, UniquePropertyValueValidationException.class );
        commit();
    }

    @Test
    public void shouldEnforceOnSetLabelInTx() throws Exception
    {
        // given
        createNodeWithLabelAndProps( label, aValues );

        // when
        newTransaction();
        long nodeB = createNode();
        setProperties( nodeB, bValues );

        assertException( () ->
        {
            addLabel( nodeB, label ); // boom!

        }, UniquePropertyValueValidationException.class );
        commit();
    }

    private void newTransaction() throws KernelException
    {
        if ( transaction != null )
        {
            fail( "tx already opened" );
        }
        transaction = session.beginTransaction( KernelTransaction.Type.implicit );
    }

    protected void commit() throws TransactionFailureException
    {
        transaction.success();
        try
        {
            transaction.close();
        }
        finally
        {
            transaction = null;
        }
    }

    private long createLabeledNode( int labelId ) throws KernelException
    {
        long node = transaction.dataWrite().nodeCreate();
        transaction.dataWrite().nodeAddLabel( node, labelId );
        return node;
    }

    private void addLabel( long nodeId, int labelId ) throws KernelException
    {
        transaction.dataWrite().nodeAddLabel( nodeId, labelId );
    }

    private void setProperty( long nodeId, int propertyId, Object value ) throws KernelException
    {
        transaction.dataWrite().nodeSetProperty( nodeId, propertyId, Values.of( value ) );
    }

    private long createNode() throws KernelException
    {
        return transaction.dataWrite().nodeCreate();
    }

    private long createNodeWithLabelAndProps( int labelId, Object[] propertyValues )
            throws KernelException
    {
        newTransaction();
        long nodeId = createNode();
        addLabel( nodeId, labelId );
        for ( int prop = 0; prop < numberOfProps; prop++ )
        {
            setProperty( nodeId, prop, propertyValues[prop] );
        }
        commit();
        return nodeId;
    }

    private void setProperties( long nodeId, Object[] propertyValues )
            throws KernelException
    {
        for ( int prop = 0; prop < propertyValues.length; prop++ )
        {
            setProperty( nodeId, prop, propertyValues[prop] );
        }
    }

    private int[] propertyIds()
    {
        int[] props = new int[numberOfProps];
        for ( int i = 0; i < numberOfProps; i++ )
        {
            props[i] = i;
        }
        return props;
    }

    static class TestParams // Only here to be able to produce readable output
    {
        private final Object[] lhs;
        private final Object[] rhs;

        TestParams( Object[] lhs, Object[] rhs )
        {
            this.lhs = lhs;
            this.rhs = rhs;
        }

        @Override
        public String toString()
        {
            return String.format( "lhs=%s, rhs=%s", ArrayUtils.toString( lhs ), ArrayUtils.toString( rhs ) );
        }
    }
}
