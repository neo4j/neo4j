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
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.values.storable.Values;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Iterators.single;
import static org.neo4j.kernel.impl.api.store.DefaultIndexReference.fromDescriptor;

public class UniquenessConstraintCreationIT
        extends AbstractConstraintCreationIT<ConstraintDescriptor,LabelSchemaDescriptor>
{
    private static final String DUPLICATED_VALUE = "apa";
    private SchemaIndexDescriptor uniqueIndex;

    @Override
    int initializeLabelOrRelType( TokenWrite tokenWrite, String name ) throws KernelException
    {
        return tokenWrite.labelGetOrCreateForName( KEY );
    }

    @Override
    ConstraintDescriptor createConstraint( SchemaWrite writeOps, LabelSchemaDescriptor descriptor )
            throws Exception
    {
        return writeOps.uniquePropertyConstraintCreate( descriptor );
    }

    @Override
    void createConstraintInRunningTx( GraphDatabaseService db, String type, String property )
    {
        SchemaHelper.createUniquenessConstraint( db, type, property );
    }

    @Override
    UniquenessConstraintDescriptor newConstraintObject( LabelSchemaDescriptor descriptor )
    {
        return ConstraintDescriptorFactory.uniqueForSchema( descriptor );
    }

    @Override
    void dropConstraint( SchemaWrite writeOps, ConstraintDescriptor constraint ) throws Exception
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

    @Override
    LabelSchemaDescriptor makeDescriptor( int typeId, int propertyKeyId )
    {
        uniqueIndex = SchemaIndexDescriptorFactory.uniqueForLabel( typeId, propertyKeyId );
        return SchemaDescriptorFactory.forLabel( typeId, propertyKeyId );
    }

    @Test
    public void shouldAbortConstraintCreationWhenDuplicatesExist() throws Exception
    {
        // given
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
        // name is not unique for Foo in the existing data

        int foo = transaction.tokenWrite().labelGetOrCreateForName( "Foo" );
        int name = transaction.tokenWrite().propertyKeyGetOrCreateForName( "name" );

        long node1 = transaction.dataWrite().nodeCreate();

        transaction.dataWrite().nodeAddLabel( node1, foo );
        transaction.dataWrite().nodeSetProperty( node1, name, Values.of( "foo" ) );

        long node2 = transaction.dataWrite().nodeCreate();
        transaction.dataWrite().nodeAddLabel( node2, foo );

        transaction.dataWrite().nodeSetProperty( node2, name, Values.of( "foo" ) );
        commit();

        // when
        LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( foo, name );
        try
        {
            SchemaWrite schemaWriteOperations = schemaWriteInNewTransaction();
            schemaWriteOperations.uniquePropertyConstraintCreate( descriptor );

            fail( "expected exception" );
        }
        // then
        catch ( CreateConstraintFailureException ex )
        {
            assertEquals( ConstraintDescriptorFactory.uniqueForSchema( descriptor ), ex.constraint() );
            Throwable cause = ex.getCause();
            assertThat( cause, instanceOf( ConstraintValidationException.class ) );

            String expectedMessage = String.format(
                    "Both Node(%d) and Node(%d) have the label `Foo` and property `name` = 'foo'", node1, node2 );
            String actualMessage = userMessage( (ConstraintValidationException) cause );
            assertEquals( expectedMessage, actualMessage );
        }
    }

    @Test
    public void shouldCreateAnIndexToGoAlongWithAUniquePropertyConstraint() throws Exception
    {
        // when
        SchemaWrite schemaWriteOperations = schemaWriteInNewTransaction();
        schemaWriteOperations.uniquePropertyConstraintCreate( descriptor );
        commit();

        // then
        Transaction transaction = newTransaction();
        assertEquals( asSet( fromDescriptor( uniqueIndex ) ), asSet( transaction.schemaRead().indexesGetAll() ) );
        commit();
    }

    @Test
    public void shouldDropCreatedConstraintIndexWhenRollingBackConstraintCreation() throws Exception
    {
        // given
        Transaction transaction = newTransaction( LoginContext.AUTH_DISABLED );
        transaction.schemaWrite().uniquePropertyConstraintCreate( descriptor );
        assertEquals( asSet( fromDescriptor( uniqueIndex ) ), asSet( transaction.schemaRead().indexesGetAll() ) );

        // when
        rollback();

        // then
        transaction = newTransaction();
        assertEquals( emptySet(), asSet( transaction.schemaRead().indexesGetAll() ) );
        commit();
    }

    @Test
    public void shouldNotDropUniquePropertyConstraintThatDoesNotExistWhenThereIsAPropertyExistenceConstraint()
            throws Exception
    {
        // given
        SchemaWrite schemaWriteOperations = schemaWriteInNewTransaction();
        schemaWriteOperations.nodePropertyExistenceConstraintCreate( descriptor );
        commit();

        // when
        try
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.constraintDrop( ConstraintDescriptorFactory.uniqueForSchema( descriptor ) );

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
            Transaction transaction = newTransaction();

            Iterator<ConstraintDescriptor> constraints = transaction.schemaRead().constraintsGetForSchema( descriptor );

            assertEquals( ConstraintDescriptorFactory.existsForSchema( descriptor ), single( constraints ) );
            commit();
        }
    }

    @Test
    public void committedConstraintRuleShouldCrossReferenceTheCorrespondingIndexRule() throws Exception
    {
        // when
        SchemaWrite statement = schemaWriteInNewTransaction();
        statement.uniquePropertyConstraintCreate( descriptor );
        commit();

        // then
        SchemaStorage schema = new SchemaStorage( neoStores().getSchemaStore() );
        IndexRule indexRule = schema.indexGetForSchema( SchemaIndexDescriptorFactory
                .uniqueForLabel( typeId, propertyKeyId ) );
        ConstraintRule constraintRule = schema.constraintsGetSingle(
                ConstraintDescriptorFactory.uniqueForLabel( typeId, propertyKeyId ) );
        assertEquals( constraintRule.getId(), indexRule.getOwningConstraint().longValue() );
        assertEquals( indexRule.getId(), constraintRule.getOwnedIndex() );
    }

    private NeoStores neoStores()
    {
        return db.getDependencyResolver().resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
    }

    @Test
    public void shouldDropConstraintIndexWhenDroppingConstraint() throws Exception
    {
        // given
        Transaction transaction = newTransaction( LoginContext.AUTH_DISABLED );
        ConstraintDescriptor constraint =
                transaction.schemaWrite().uniquePropertyConstraintCreate( descriptor );
        assertEquals( asSet( fromDescriptor( uniqueIndex ) ), asSet( transaction.schemaRead().indexesGetAll() ) );
        commit();

        // when
        SchemaWrite schemaWriteOperations = schemaWriteInNewTransaction();
        schemaWriteOperations.constraintDrop( constraint );
        commit();

        // then
        transaction = newTransaction();
        assertEquals( emptySet(), asSet( transaction.schemaRead().indexesGetAll() ) );
        commit();
    }

    private String userMessage( ConstraintValidationException cause )
            throws TransactionFailureException
    {
        try ( Transaction tx = newTransaction() )
        {
            TokenNameLookup lookup = new SilentTokenNameLookup( tx.tokenRead() );
            return cause.getUserMessage( lookup );
        }
    }
}
