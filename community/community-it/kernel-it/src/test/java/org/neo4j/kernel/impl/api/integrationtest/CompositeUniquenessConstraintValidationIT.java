 /*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;

@ImpermanentDbmsExtension
@TestInstance( TestInstance.Lifecycle.PER_CLASS )
public class CompositeUniquenessConstraintValidationIT
{
    @Inject
    private GraphDatabaseAPI db;

    private ConstraintDescriptor constraintDescriptor;
    private int label;
    private KernelTransaction transaction;
    protected Kernel kernel;

    public static Stream<Arguments> parameterValues()
    {
        return Stream.of(
                Arguments.of( values( 10 ), values( 10d ) ),
                Arguments.of( values( 10, 20 ), values( 10, 20 ) ),
                Arguments.of( values( 10L, 20L ), values( 10, 20 ) ),
                Arguments.of( values( 10, 20 ), values( 10L, 20L ) ),
                Arguments.of( values( 10, 20 ), values( 10.0, 20.0 ) ),
                Arguments.of( values( 10, 20 ), values( 10.0, 20.0 ) ),
                Arguments.of( values( new int[]{1, 2}, "v2" ), values( new int[]{1, 2}, "v2" ) ),
                Arguments.of( values( "a", "b", "c" ), values( "a", "b", "c" ) ),
                Arguments.of( values( 285414114323346805L ), values( 285414114323346805L ) ),
                Arguments.of( values( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ), values( 1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d ) )
        );
    }

    private static Object[] values( Object... values )
    {
        return values;
    }

    @BeforeEach
    public void setup() throws Exception
    {
        kernel = db.getDependencyResolver().resolveDependency( Kernel.class );

        newTransaction();
        // This transaction allocates all the tokens we'll need in this test.
        // We rely on token ids being allocated sequentially, from and including zero.
        TokenWrite tokenWrite = transaction.tokenWrite();
        tokenWrite.labelGetOrCreateForName( "Label0" );
        label = tokenWrite.labelGetOrCreateForName( "Label1" );
        assertEquals( 1, label );
        for ( int i = 0; i < 10; i++ )
        {
            int prop = tokenWrite.propertyKeyGetOrCreateForName( "prop" + i );
            assertEquals( i, prop );
        }
        commit();
}

    private void setupConstraintDescriptor( int nbrOfProperties ) throws KernelException
    {
        newTransaction();
        constraintDescriptor =
                transaction.schemaWrite().uniquePropertyConstraintCreate( IndexPrototype.uniqueForSchema( forLabel( label, propertyIds( nbrOfProperties ) ) ) );
        commit();
    }

    @AfterEach
    public void clean() throws Exception
    {
        if ( transaction != null )
        {
            transaction.close();
            transaction = null;
        }

        newTransaction();
        transaction.schemaWrite().constraintDrop( constraintDescriptor );
        commit();

        try ( KernelTransaction tx = kernel.beginTransaction( KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED ) )
        {
            try ( NodeCursor node = tx.cursors().allocateNodeCursor( tx.pageCursorTracer() ) )
            {
                tx.dataRead().allNodesScan( node );
                while ( node.next() )
                {
                    tx.dataWrite().nodeDelete( node.nodeReference() );
                }
            }
            tx.commit();
        }
    }

    @ParameterizedTest( name = "{index}: lhs={0}, rhs={1}" )
    @MethodSource( "parameterValues" )
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_DeleteNode( Object[] lhs, Object[] rhs ) throws Exception
    {
        setupConstraintDescriptor( lhs.length );

        // given
        long node = createNodeWithLabelAndProps( label, lhs );

        // when
        newTransaction();
        transaction.dataWrite().nodeDelete( node );
        long newNode = createLabeledNode( label );
        setProperties( newNode, lhs );

        // then does not fail
        commit();
    }

    @ParameterizedTest( name = "{index}: lhs={0}, rhs={1}" )
    @MethodSource( "parameterValues" )
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_RemoveLabel( Object[] lhs, Object[] rhs ) throws Exception
    {
        setupConstraintDescriptor( lhs.length );

        // given
        long node = createNodeWithLabelAndProps( label, lhs );

        // when
        newTransaction();
        transaction.dataWrite().nodeRemoveLabel( node, label );
        long newNode = createLabeledNode( label );
        setProperties( newNode, lhs );

        // then does not fail
        commit();
    }

    @ParameterizedTest( name = "{index}: lhs={0}, rhs={1}" )
    @MethodSource( "parameterValues" )
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_RemoveProperty( Object[] lhs, Object[] rhs ) throws Exception
    {
        setupConstraintDescriptor( lhs.length );

        // given
        long node = createNodeWithLabelAndProps( label, lhs );

        // when
        newTransaction();
        transaction.dataWrite().nodeRemoveProperty( node, 0 );
        long newNode = createLabeledNode( label );
        setProperties( newNode, lhs );

        // then does not fail
        commit();
    }

    @ParameterizedTest( name = "{index}: lhs={0}, rhs={1}" )
    @MethodSource( "parameterValues" )
    public void shouldAllowRemoveAndAddConflictingDataInOneTransaction_ChangeProperty( Object[] lhs, Object[] rhs ) throws Exception
    {
        setupConstraintDescriptor( lhs.length );

        // given
        long node = createNodeWithLabelAndProps( label, lhs );

        // when
        newTransaction();
        transaction.dataWrite().nodeSetProperty( node, 0, Values.of( "Alive!" ) );
        long newNode = createLabeledNode( label );
        setProperties( newNode, lhs );

        // then does not fail
        commit();
    }

    @ParameterizedTest( name = "{index}: lhs={0}, rhs={1}" )
    @MethodSource( "parameterValues" )
    public void shouldPreventConflictingDataInTx( Object[] lhs, Object[] rhs ) throws Throwable
    {
        setupConstraintDescriptor( lhs.length );

        // Given

        // When
        newTransaction();
        long n1 = createLabeledNode( label );
        long n2 = createLabeledNode( label );
        setProperties( n1, lhs );
        int lastPropertyOffset = lhs.length - 1;
        for ( int prop = 0; prop < lastPropertyOffset; prop++ )
        {
            setProperty( n2, prop, lhs[prop] ); // still ok
        }

        assertThatThrownBy( () -> setProperty( n2, lastPropertyOffset, lhs[lastPropertyOffset] ) ).isInstanceOf(
                UniquePropertyValueValidationException.class );

        // Then should fail
        commit();
    }

    @ParameterizedTest( name = "{index}: lhs={0}, rhs={1}" )
    @MethodSource( "parameterValues" )
    public void shouldEnforceOnSetProperty( Object[] lhs, Object[] rhs ) throws Exception
    {
        setupConstraintDescriptor( lhs.length );

        // given
        createNodeWithLabelAndProps( label, lhs );

        // when
        newTransaction();
        long node = createLabeledNode( label );

        int lastPropertyOffset = lhs.length - 1;
        for ( int prop = 0; prop < lastPropertyOffset; prop++ )
        {
            setProperty( node, prop, lhs[prop] ); // still ok
        }

        assertThatThrownBy( () -> setProperty( node, lastPropertyOffset, lhs[lastPropertyOffset] ) ).isInstanceOf(
                UniquePropertyValueValidationException.class );
        commit();
    }

    @ParameterizedTest( name = "{index}: lhs={0}, rhs={1}" )
    @MethodSource( "parameterValues" )
    public void shouldEnforceOnSetLabel( Object[] lhs, Object[] rhs ) throws Exception
    {
        setupConstraintDescriptor( lhs.length );

        // given
        createNodeWithLabelAndProps( label, lhs );

        // when
        newTransaction();
        long node = createNode();
        setProperties( node, rhs ); // ok because no label is set

        assertThatThrownBy( () -> addLabel( node, label ) ).isInstanceOf( UniquePropertyValueValidationException.class );
        commit();
    }

    @ParameterizedTest( name = "{index}: lhs={0}, rhs={1}" )
    @MethodSource( "parameterValues" )
    public void shouldEnforceOnSetPropertyInTx( Object[] lhs, Object[] rhs ) throws Exception
    {
        setupConstraintDescriptor( lhs.length );

        // when
        newTransaction();
        long aNode = createLabeledNode( label );
        setProperties( aNode, lhs );

        long nodeB = createLabeledNode( label );
        int lastPropertyOffset = lhs.length - 1;
        for ( int prop = 0; prop < lastPropertyOffset; prop++ )
        {
            setProperty( nodeB, prop, rhs[prop] ); // still ok
        }

        assertThatThrownBy( () -> setProperty( nodeB, lastPropertyOffset, rhs[lastPropertyOffset] ) )
                .isInstanceOf( UniquePropertyValueValidationException.class );
        commit();
    }

    @ParameterizedTest( name = "{index}: lhs={0}, rhs={1}" )
    @MethodSource( "parameterValues" )
    public void shouldEnforceOnSetLabelInTx( Object[] lhs, Object[] rhs ) throws Exception
    {
        setupConstraintDescriptor( lhs.length );

        // given
        createNodeWithLabelAndProps( label, lhs );

        // when
        newTransaction();
        long nodeB = createNode();
        setProperties( nodeB, rhs );

        assertThatThrownBy( () -> addLabel( nodeB, label ) ).isInstanceOf( UniquePropertyValueValidationException.class );
        commit();
    }

    private void newTransaction() throws KernelException
    {
        assertThat( transaction ).as("tx already opened").isNull();
        transaction = kernel.beginTransaction( KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED );
    }

    protected void commit() throws TransactionFailureException
    {
        transaction.commit();
        transaction = null;
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
        for ( int prop = 0; prop < propertyValues.length; prop++ )
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

    private int[] propertyIds( int numberOfProps )
    {
        int[] props = new int[numberOfProps];
        for ( int i = 0; i < numberOfProps; i++ )
        {
            props[i] = i;
        }
        return props;
    }
}
