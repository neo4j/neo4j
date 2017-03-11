/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.schema_new.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.security.SecurityContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.api.properties.Property.property;
import static org.neo4j.kernel.api.schema_new.SchemaDescriptorFactory.forLabel;
import static org.neo4j.test.assertion.Assert.assertException;

@RunWith( Parameterized.class )
public class CompositeUniquenessConstraintValidationIT
{
    @ClassRule
    public static ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    @Rule
    public final TestName testName = new TestName();

    @Parameterized.Parameters( name = "Collide ({0},{1}) with ({2},{3})" )
    public static Iterable<Object[]> parameterValues() throws IOException
    {
        return Arrays.<Object[]>asList(
                Iterators.array( "v1", "v2", "v1", "v2" ),
                Iterators.array( 10, 20, 10, 20 ),
                Iterators.array( 10L, 20L, 10, 20 ),
                Iterators.array( 10, 20, 10L, 20L ),
                Iterators.array( 10, 20, 10.0, 20.0 ),
                Iterators.array( new int[]{1,2}, "v2", new int[]{1,2}, "v2" ),
                Iterators.array( 'v', "v2", "v", "v2" )
        );
    }

    private static final int label = 1;
    private static final int prop1 = 2;
    private static final int prop2 = 3;

    private final Object valueA1;
    private final Object valueA2;

    private final Object valueB1;
    private final Object valueB2;

    public CompositeUniquenessConstraintValidationIT( Object valueA1, Object valueA2, Object valueB1, Object valueB2 )
    {
        this.valueA1 = valueA1;
        this.valueA2 = valueA2;
        this.valueB1 = valueB1;
        this.valueB2 = valueB2;
    }

    private KernelTransaction transaction;
    private Statement statement;
    private GraphDatabaseAPI graphDatabaseAPI;
    protected KernelAPI kernel;

    @Before
    public void setup() throws Exception
    {
        graphDatabaseAPI = dbRule.getGraphDatabaseAPI();
        kernel = graphDatabaseAPI.getDependencyResolver().resolveDependency( KernelAPI.class );

        newTransaction();
        statement.schemaWriteOperations().uniquePropertyConstraintCreate( forLabel( label, prop1, prop2 ) );
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
        statement.schemaWriteOperations().constraintDrop(
                ConstraintDescriptorFactory.uniqueForLabel( label, prop1, prop2 ) );
        commit();

        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            for ( Node node : graphDatabaseAPI.getAllNodes() )
            {
                node.delete();
            }
            tx.success();
        }
    }

    @Test
    public void shouldEnforceOnSetProperty() throws Exception
    {
        // given
        createNode( label, MapUtil.genericMap( prop1, valueA1, prop2, valueA2 ) );

        // when
        newTransaction();
        long node = createLabeledNode( label );
        assertException( () -> {
            setProperty( node, prop1, valueB1 ); // still ok
            setProperty( node, prop2, valueB2 ); // boom!

        }, UniquePropertyValueValidationException.class, "" );
        commit();
    }

    @Test
    public void shouldEnforceOnSetLabel() throws Exception
    {
        // given
        createNode( label, MapUtil.genericMap( prop1, valueA1, prop2, valueA2 ) );

        // when
        newTransaction();
        long node = createNode();
        assertException( () -> {
            setProperty( node, prop1, valueB1 ); // still ok
            setProperty( node, prop2, valueB2 ); // and fine
            addLabel( node, label ); // boom again

        }, UniquePropertyValueValidationException.class, "" );
        commit();
    }

    @Test
    public void shouldEnforceOnSetPropertyInTx() throws Exception
    {
        // when
        newTransaction();
        long nodeA = createLabeledNode( label );
        setProperty( nodeA, prop1, valueA1 );
        setProperty( nodeA, prop2, valueA2 );

        long nodeB = createLabeledNode( label );
        assertException( () -> {
            setProperty( nodeB, prop1, valueB1 ); // still ok
            setProperty( nodeB, prop2, valueB2 ); // boom!

        }, UniquePropertyValueValidationException.class, "" );
        commit();
    }

    @Test
    public void shouldEnforceOnSetLabelInTx() throws Exception
    {
        // when
        newTransaction();
        long nodeA = createLabeledNode( label );
        setProperty( nodeA, prop1, valueA1 );
        setProperty( nodeA, prop2, valueA2 );

        long nodeB = createNode();
        assertException( () -> {
            setProperty( nodeB, prop1, valueB1 ); // still ok
            setProperty( nodeB, prop2, valueB2 ); // and fine
            addLabel( nodeB, label ); // boom again

        }, UniquePropertyValueValidationException.class, "" );
        commit();
    }

    private void newTransaction() throws KernelException
    {
        transaction = kernel.newTransaction( KernelTransaction.Type.implicit, SecurityContext.AUTH_DISABLED );
        statement = transaction.acquireStatement();
    }

    protected void commit() throws TransactionFailureException
    {
        statement.close();
        statement = null;
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
        long node = statement.dataWriteOperations().nodeCreate();
        statement.dataWriteOperations().nodeAddLabel( node, labelId );
        return node;
    }

    private void addLabel( long nodeId, int labelId ) throws KernelException
    {
        statement.dataWriteOperations().nodeAddLabel( nodeId, labelId );
    }

    private void setProperty( long nodeId, int propertyId, Object value ) throws KernelException
    {
        statement.dataWriteOperations().nodeSetProperty( nodeId, property( propertyId, value ) );
    }

    private long createNode() throws KernelException
    {
        return statement.dataWriteOperations().nodeCreate();
    }

    private long createNode( int labelId, Map<Integer,Object> properties )
            throws KernelException
    {
        newTransaction();
        long nodeId = createNode();
        addLabel( nodeId, labelId );
        for ( Map.Entry<Integer,Object> entry : properties.entrySet() )
        {
            setProperty( nodeId, entry.getKey(), entry.getValue() );
        }
        commit();
        return nodeId;
    }
}
