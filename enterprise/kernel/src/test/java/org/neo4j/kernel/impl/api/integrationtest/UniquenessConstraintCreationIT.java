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
package org.neo4j.kernel.impl.api.integrationtest;

import org.junit.Test;

import java.util.Iterator;

import org.neo4j.SchemaHelper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.constraints.NodePropertyConstraint;
import org.neo4j.kernel.api.constraints.NodePropertyExistenceConstraint;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.UniquePropertyConstraintRule;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptySetOf;
import static org.neo4j.helpers.collection.IteratorUtil.single;

public class UniquenessConstraintCreationIT extends AbstractConstraintCreationIT<UniquenessConstraint>
{
    private static final String DUPLICATED_VALUE = "apa";

    @Override
    int initializeLabelOrRelType( SchemaWriteOperations writeOps, String name ) throws KernelException
    {
        return writeOps.labelGetOrCreateForName( KEY );
    }

    @Override
    UniquenessConstraint createConstraint( SchemaWriteOperations writeOps, int type, int property ) throws Exception
    {
        return writeOps.uniquePropertyConstraintCreate( type, property );
    }

    @Override
    void createConstraintInRunningTx( GraphDatabaseService db, String type, String property )
    {
        SchemaHelper.createUniquenessConstraint( db, type, property );
    }

    @Override
    UniquenessConstraint newConstraintObject( int type, int property )
    {
        return new UniquenessConstraint( type, property );
    }

    @Override
    void dropConstraint( SchemaWriteOperations writeOps, UniquenessConstraint constraint ) throws Exception
    {
        writeOps.constraintDrop( constraint );
    }

    @Override
    void createOffendingDataInRunningTx( GraphDatabaseService db )
    {
        db.createNode( label( KEY ) ).setProperty( PROP, DUPLICATED_VALUE );
        db.createNode( label( KEY ) ).setProperty( PROP, DUPLICATED_VALUE );
    }

    @Override
    void removeOffendingDataInRunningTx( GraphDatabaseService db )
    {
        try ( ResourceIterator<Node> nodes = db.findNodes( label( KEY ), PROP, DUPLICATED_VALUE ) )
        {
            while ( nodes.hasNext() )
            {
                nodes.next().delete();
            }
        }
    }

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
            statement.uniquePropertyConstraintCreate( foo, name );

            fail( "expected exception" );
        }
        // then
        catch ( CreateConstraintFailureException ex )
        {
            assertEquals( new UniquenessConstraint( foo, name ), ex.constraint() );
            Throwable cause = ex.getCause();
            assertThat( cause, instanceOf( ConstraintVerificationFailedKernelException.class ) );

            String expectedMessage = String.format(
                    "Multiple nodes with label `%s` have property `%s` = '%s':%n  node(%d)%n  node(%d)",
                    "Foo", "name", "foo", node1, node2 );
            String actualMessage = userMessage( (ConstraintVerificationFailedKernelException) cause );
            assertEquals( expectedMessage, actualMessage );
        }
    }

    @Test
    public void shouldCreateAnIndexToGoAlongWithAUniquePropertyConstraint() throws Exception
    {
        // when
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.uniquePropertyConstraintCreate( typeId, propertyKeyId );
            commit();
        }

        // then
        {
            ReadOperations statement = readOperationsInNewTransaction();
            assertEquals( asSet( new IndexDescriptor( typeId, propertyKeyId ) ),
                    asSet( statement.uniqueIndexesGetAll() ) );
        }
    }

    @Test
    public void shouldDropCreatedConstraintIndexWhenRollingBackConstraintCreation() throws Exception
    {
        // given
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.uniquePropertyConstraintCreate( typeId, propertyKeyId );
            assertEquals( asSet( new IndexDescriptor( typeId, propertyKeyId ) ),
                    asSet( statement.uniqueIndexesGetAll() ) );
        }

        // when
        rollback();

        // then
        {
            ReadOperations statement = readOperationsInNewTransaction();
            assertEquals( emptySetOf( IndexDescriptor.class ), asSet( statement.uniqueIndexesGetAll() ) );
            commit();
        }
    }

    @Test
    public void shouldNotDropUniquePropertyConstraintThatDoesNotExistWhenThereIsAPropertyExistenceConstraint()
            throws Exception
    {
        // given
        NodePropertyExistenceConstraint constraint;
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            constraint = statement.nodePropertyExistenceConstraintCreate( typeId, propertyKeyId );
            commit();
        }

        // when
        try
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.constraintDrop( new UniquenessConstraint( constraint.label(), constraint.propertyKey() ) );

            fail( "expected exception" );
        }
        // then
        catch ( DropConstraintFailureException e )
        {
            assertThat( e.getCause(), instanceOf( NoSuchConstraintException.class ) );
        }
        finally
        {
            rollback();
        }

        // then
        {
            ReadOperations statement = readOperationsInNewTransaction();

            Iterator<NodePropertyConstraint> constraints =
                    statement.constraintsGetForLabelAndPropertyKey( typeId, propertyKeyId );

            assertEquals( constraint, single( constraints ) );
        }
    }

    @Test
    public void committedConstraintRuleShouldCrossReferenceTheCorrespondingIndexRule() throws Exception
    {
        // when
        SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
        statement.uniquePropertyConstraintCreate( typeId, propertyKeyId );
        commit();

        // then
        SchemaStorage schema = new SchemaStorage( neoStores().getSchemaStore() );
        IndexRule indexRule = schema.indexRule( typeId, propertyKeyId );
        UniquePropertyConstraintRule constraintRule = schema.uniquenessConstraint( typeId, propertyKeyId );
        assertEquals( constraintRule.getId(), indexRule.getOwningConstraint().longValue() );
        assertEquals( indexRule.getId(), constraintRule.getOwnedIndex() );
    }

    private NeoStores neoStores()
    {
        return db.getDependencyResolver().resolveDependency( NeoStores.class );
    }

    @Test
    public void shouldDropConstraintIndexWhenDroppingConstraint() throws Exception
    {
        // given
        UniquenessConstraint constraint;
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            constraint = statement.uniquePropertyConstraintCreate( typeId, propertyKeyId );
            assertEquals( asSet( new IndexDescriptor( typeId, propertyKeyId ) ),
                    asSet( statement.uniqueIndexesGetAll() ) );
            commit();
        }

        // when
        {
            SchemaWriteOperations statement = schemaWriteOperationsInNewTransaction();
            statement.constraintDrop( constraint );
            commit();
        }

        // then
        {
            ReadOperations statement = readOperationsInNewTransaction();
            assertEquals( emptySetOf( IndexDescriptor.class ), asSet( statement.uniqueIndexesGetAll() ) );
            commit();
        }
    }

    private String userMessage( ConstraintVerificationFailedKernelException cause )
            throws TransactionFailureException
    {
        StatementTokenNameLookup lookup = new StatementTokenNameLookup( readOperationsInNewTransaction() );
        String actualMessage = cause.getUserMessage( lookup );
        commit();
        return actualMessage;
    }
}
