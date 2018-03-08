/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.coreapi.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.index.IndexPopulationProgress;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.internal.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.StatementTokenNameLookup;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedPropertyInCompositeSchemaException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.constaints.NodeExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.NodeKeyConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.RelExistenceConstraintDescriptor;
import org.neo4j.kernel.api.schema.constaints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.storageengine.api.schema.PopulationProgress;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.schema.Schema.IndexState.FAILED;
import static org.neo4j.graphdb.schema.Schema.IndexState.ONLINE;
import static org.neo4j.graphdb.schema.Schema.IndexState.POPULATING;
import static org.neo4j.helpers.collection.Iterators.addToCollection;
import static org.neo4j.helpers.collection.Iterators.asCollection;
import static org.neo4j.helpers.collection.Iterators.map;
import static org.neo4j.kernel.impl.coreapi.schema.PropertyNameUtils.getOrCreatePropertyKeyIds;

public class SchemaImpl implements Schema
{
    private final Supplier<KernelTransaction> transactionSupplier;
    private final InternalSchemaActions actions;

    public SchemaImpl( Supplier<KernelTransaction> transactionSupplier )
    {
        this.transactionSupplier = transactionSupplier;
        //TODO this should be updated once writes are ported
        this.actions = new GDBSchemaActions( () -> transactionSupplier.get().acquireStatement() );
    }

    @Override
    public IndexCreator indexFor( Label label )
    {
        return new IndexCreatorImpl( actions, label );
    }

    @Override
    public Iterable<IndexDefinition> getIndexes( final Label label )
    {
        KernelTransaction transaction = transactionSupplier.get();
        try ( Statement ignore = transaction.acquireStatement() )
        {
            TokenRead tokenRead = transaction.tokenRead();
            SchemaRead schemaRead = transaction.schemaRead();
            List<IndexDefinition> definitions = new ArrayList<>();
            int labelId = tokenRead.nodeLabel( label.name() );
            if ( labelId == TokenRead.NO_TOKEN )
            {
                return emptyList();
            }
            Iterator<CapableIndexReference> indexes = schemaRead.indexesGetForLabel( labelId );
            addDefinitions( definitions, tokenRead, CapableIndexReference.sortByType( indexes ) );
            return definitions;
        }
    }

    @Override
    public Iterable<IndexDefinition> getIndexes()
    {
        KernelTransaction transaction = transactionSupplier.get();
        SchemaRead schemaRead = transaction.schemaRead();
        try ( Statement ignore = transaction.acquireStatement() )
        {
            List<IndexDefinition> definitions = new ArrayList<>();

            Iterator<CapableIndexReference> indexes = schemaRead.indexesGetAll();
            addDefinitions( definitions, transaction.tokenRead(), CapableIndexReference.sortByType( indexes ) );
            return definitions;
        }
    }

    private IndexDefinition descriptorToDefinition( final TokenRead tokenRead, CapableIndexReference index )
    {
        try
        {
            Label label = label( tokenRead.nodeLabelName( index.label() ) );
            boolean constraintIndex = index.isUnique();
            String[] propertyNames = PropertyNameUtils.getPropertyKeys( tokenRead, index.properties() );
            return new IndexDefinitionImpl( actions, label, propertyNames, constraintIndex );
        }
        catch ( LabelNotFoundKernelException | PropertyKeyIdNotFoundKernelException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void addDefinitions( List<IndexDefinition> definitions, final TokenRead tokenRead,
                                 Iterator<CapableIndexReference> indexes )
    {
        addToCollection(
                map( index -> descriptorToDefinition( tokenRead, index ), indexes ),
                definitions );
    }

    @Override
    public void awaitIndexOnline( IndexDefinition index, long duration, TimeUnit unit )
    {
        actions.assertInOpenTransaction();
        long timeout = System.currentTimeMillis() + unit.toMillis( duration );
        do
        {
            IndexState state = getIndexState( index );
            switch ( state )
            {
            case ONLINE:
                return;
            case FAILED:
                throw new IllegalStateException( "Index entered a FAILED state. Please see database logs." );
            default:
                try
                {
                    Thread.sleep( 100 );
                }
                catch ( InterruptedException e )
                {   // What to do?
                }
                break;
            }
        } while ( System.currentTimeMillis() < timeout );
        throw new IllegalStateException( "Expected index to come online within a reasonable time." );
    }

    @Override
    public void awaitIndexesOnline( long duration, TimeUnit unit )
    {
        actions.assertInOpenTransaction();
        long millisLeft = TimeUnit.MILLISECONDS.convert( duration, unit );
        Collection<IndexDefinition> onlineIndexes = new ArrayList<>();

        for ( Iterator<IndexDefinition> iter = getIndexes().iterator(); iter.hasNext(); )
        {
            if ( millisLeft < 0 )
            {
                throw new IllegalStateException( "Expected all indexes to come online within a reasonable time."
                                                 + "Indexes brought online: " + onlineIndexes
                                                 + ". Indexes not guaranteed to be online: " + asCollection( iter ) );
            }

            IndexDefinition index = iter.next();
            long millisBefore = System.currentTimeMillis();
            awaitIndexOnline( index, millisLeft, TimeUnit.MILLISECONDS );
            millisLeft -= System.currentTimeMillis() - millisBefore;

            onlineIndexes.add( index );
        }
    }

    @Override
    public IndexState getIndexState( final IndexDefinition index )
    {
        KernelTransaction transaction = safeAcquireTransaction();
        try ( Statement ignore = transaction.acquireStatement() )
        {

            SchemaRead schemaRead = transaction.schemaRead();
            CapableIndexReference reference = getIndexReference( schemaRead, transaction.tokenRead(), index );
            InternalIndexState indexState = schemaRead.indexGetState( reference );
            switch ( indexState )
            {
                case POPULATING:
                    return POPULATING;
                case ONLINE:
                    return ONLINE;
                case FAILED:
                    return FAILED;
                default:
                    throw new IllegalArgumentException( String.format( "Illegal index state %s", indexState ) );
            }
        }
        catch ( SchemaRuleNotFoundException | IndexNotFoundKernelException e )
        {
            throw new NotFoundException( format( "No index for label %s on property %s",
                    index.getLabel().name(), index.getPropertyKeys() ) );
        }
    }

    @Override
    public IndexPopulationProgress getIndexPopulationProgress( IndexDefinition index )
    {
        KernelTransaction transaction = safeAcquireTransaction();
        try ( Statement ignore = transaction.acquireStatement() )
        {
            SchemaRead schemaRead = transaction.schemaRead();
            CapableIndexReference descriptor = getIndexReference( schemaRead, transaction.tokenRead(), index );
            PopulationProgress progress = schemaRead.indexGetPopulationProgress( descriptor );
            return new IndexPopulationProgress( progress.getCompleted(), progress.getTotal() );
        }
        catch ( SchemaRuleNotFoundException | IndexNotFoundKernelException e )
        {
            throw new NotFoundException( format( "No index for label %s on property %s", index.getLabel().name(),
                    index.getPropertyKeys() ) );
        }
    }

    @Override
    public String getIndexFailure( IndexDefinition index )
    {
        KernelTransaction transaction = safeAcquireTransaction();
        try ( Statement ignore = transaction.acquireStatement() )
        {
            SchemaRead schemaRead = transaction.schemaRead();
            CapableIndexReference descriptor = getIndexReference( schemaRead, transaction.tokenRead(), index );
            return schemaRead.indexGetFailure( descriptor );
        }
        catch ( SchemaRuleNotFoundException | IndexNotFoundKernelException e )
        {
            throw new NotFoundException( format( "No index for label %s on property %s",
                    index.getLabel().name(), index.getPropertyKeys() ) );
        }
    }

    @Override
    public ConstraintCreator constraintFor( Label label )
    {
        actions.assertInOpenTransaction();
        return new BaseNodeConstraintCreator( actions, label );
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints()
    {
        KernelTransaction transaction = safeAcquireTransaction();
        try ( Statement ignore = transaction.acquireStatement() )
        {
            return asConstraintDefinitions( transaction.schemaRead().constraintsGetAll(), transaction.tokenRead() );
        }
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints( final Label label )
    {
        KernelTransaction transaction = safeAcquireTransaction();
        try ( Statement ignore = transaction.acquireStatement() )
        {
            TokenRead tokenRead = transaction.tokenRead();
            SchemaRead schemaRead = transaction.schemaRead();
            int labelId = tokenRead.nodeLabel( label.name() );
            if ( labelId == TokenRead.NO_TOKEN )
            {
                return emptyList();
            }
            return asConstraintDefinitions( schemaRead.constraintsGetForLabel( labelId ), tokenRead );
        }
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints( RelationshipType type )
    {
        KernelTransaction transaction = safeAcquireTransaction();
        try ( Statement ignore = transaction.acquireStatement() )
        {
            TokenRead tokenRead = transaction.tokenRead();
            SchemaRead schemaRead = transaction.schemaRead();
            int typeId = tokenRead.relationshipType( type.name() );
            if ( typeId == TokenRead.NO_TOKEN )
            {
                return emptyList();
            }
            return asConstraintDefinitions( schemaRead.constraintsGetForRelationshipType( typeId ), tokenRead );
        }
    }

    private static SchemaIndexDescriptor getIndexDescriptor( ReadOperations readOperations, IndexDefinition index )
            throws SchemaRuleNotFoundException
    {
        int labelId = readOperations.labelGetForName( index.getLabel().name() );
        int[] propertyKeyIds = PropertyNameUtils.getPropertyIds( readOperations, index.getPropertyKeys() );
        assertValidLabel( index.getLabel(), labelId );
        assertValidProperties( index.getPropertyKeys(), propertyKeyIds );
        return readOperations.indexGetForSchema( SchemaDescriptorFactory.forLabel( labelId, propertyKeyIds ) );
    }

    private static CapableIndexReference getIndexReference( SchemaRead schemaRead, TokenRead tokenRead, IndexDefinition index )
            throws SchemaRuleNotFoundException
    {
        int labelId = tokenRead.nodeLabel( index.getLabel().name() );
        int[] propertyKeyIds = PropertyNameUtils.getPropertyIds( tokenRead, index.getPropertyKeys() );
        assertValidLabel( index.getLabel(), labelId );
        assertValidProperties( index.getPropertyKeys(), propertyKeyIds );
        return schemaRead.index( labelId, propertyKeyIds );
    }

    private static void assertValidLabel( Label label, int labelId )
    {
        if ( labelId == KeyReadOperations.NO_SUCH_LABEL )
        {
            throw new NotFoundException( format( "Label %s not found", label.name() ) );
        }
    }

    private static void assertValidProperties( Iterable<String> properties , int[] propertyIds )
    {
        for ( int i = 0; i < propertyIds.length; i++ )
        {
            if ( propertyIds[i] == KeyReadOperations.NO_SUCH_PROPERTY_KEY )
            {
                throw new NotFoundException(
                        format( "Property key %s not found", Iterables.asArray( String.class, properties )[i] ) );
            }
        }
    }

    private Iterable<ConstraintDefinition> asConstraintDefinitions( Iterator<? extends ConstraintDescriptor> constraints,
           TokenRead tokenRead)
    {
        // Intentionally create an eager list so that used statement can be closed
        List<ConstraintDefinition> definitions = new ArrayList<>();

        while ( constraints.hasNext() )
        {
            ConstraintDescriptor constraint = constraints.next();
            definitions.add( asConstraintDefinition( constraint, tokenRead ) );
        }

        return definitions;
    }

    private ConstraintDefinition asConstraintDefinition( ConstraintDescriptor constraint,
            TokenRead tokenRead )
    {
        // This was turned inside out. Previously a low-level constraint object would reference a public enum type
        // which made it impossible to break out the low-level component from kernel. There could be a lower level
        // constraint type introduced to mimic the public ConstraintType, but that would be a duplicate of it
        // essentially. Checking instanceof here is OKish since the objects it checks here are part of the
        // internal storage engine API.
        SilentTokenNameLookup lookup = new SilentTokenNameLookup( tokenRead );
        if ( constraint instanceof NodeExistenceConstraintDescriptor ||
             constraint instanceof NodeKeyConstraintDescriptor ||
             constraint instanceof UniquenessConstraintDescriptor )
        {
            LabelSchemaDescriptor schemaDescriptor = (LabelSchemaDescriptor) constraint.schema();
            Label label = Label.label( lookup.labelGetName( schemaDescriptor.getLabelId() ) );
            String[] propertyKeys = Arrays.stream( schemaDescriptor.getPropertyIds() )
                    .mapToObj( lookup::propertyKeyGetName ).toArray(String[]::new);
            if ( constraint instanceof NodeExistenceConstraintDescriptor )
            {
                return new NodePropertyExistenceConstraintDefinition( actions, label, propertyKeys );
            }
            else if ( constraint instanceof UniquenessConstraintDescriptor )
            {
                return new UniquenessConstraintDefinition( actions, new IndexDefinitionImpl( actions, label,
                        propertyKeys, true ) );
            }
            else
            {
                return new NodeKeyConstraintDefinition( actions, new IndexDefinitionImpl( actions, label,
                        propertyKeys, true ) );
            }
        }
        else if ( constraint instanceof RelExistenceConstraintDescriptor )
        {
            RelationTypeSchemaDescriptor descriptor = (RelationTypeSchemaDescriptor) constraint.schema();
            return new RelationshipPropertyExistenceConstraintDefinition( actions,
                    RelationshipType.withName( lookup.relationshipTypeGetName( descriptor.getRelTypeId() ) ),
                    lookup.propertyKeyGetName( descriptor.getPropertyId() ) );
        }
        throw new IllegalArgumentException( "Unknown constraint " + constraint );
    }

    private KernelTransaction safeAcquireTransaction()
    {
        KernelTransaction transaction = transactionSupplier.get();
        if ( transaction.isTerminated() )
        {
            Status terminationReason = transaction.getReasonIfTerminated().orElse( Status.Transaction.Terminated );
            throw new TransactionTerminatedException( terminationReason );
        }
        return transaction;
    }

    private static class GDBSchemaActions implements InternalSchemaActions
    {
        private final Supplier<Statement> ctxSupplier;

        GDBSchemaActions( Supplier<Statement> statementSupplier )
        {
            this.ctxSupplier = statementSupplier;
        }

        @Override
        public IndexDefinition createIndexDefinition( Label label, String... propertyKeys )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    IndexDefinition indexDefinition = new IndexDefinitionImpl( this, label, propertyKeys, false );
                    TokenWriteOperations tokenWriteOperations = statement.tokenWriteOperations();
                    int labelId = tokenWriteOperations.labelGetOrCreateForName( indexDefinition.getLabel().name() );
                    int[] propertyKeyIds = getOrCreatePropertyKeyIds( tokenWriteOperations, indexDefinition );
                    LabelSchemaDescriptor descriptor = SchemaDescriptorFactory.forLabel( labelId, propertyKeyIds );
                    statement.schemaWriteOperations().indexCreate( descriptor );
                    return indexDefinition;
                }
                catch ( AlreadyIndexedException | AlreadyConstrainedException | RepeatedPropertyInCompositeSchemaException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
                }
                catch ( IllegalTokenNameException e )
                {
                    throw new IllegalArgumentException( e );
                }
                catch ( TooManyLabelsException e )
                {
                    throw new IllegalStateException( e );
                }
                catch ( InvalidTransactionTypeKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
            }
        }

        @Override
        public void dropIndexDefinitions( IndexDefinition indexDefinition )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    statement.schemaWriteOperations().indexDrop(
                            getIndexDescriptor( statement.readOperations(), indexDefinition ) );
                }
                catch ( NotFoundException e )
                {
                    // Silently ignore invalid label and property names
                }
                catch ( SchemaRuleNotFoundException | DropIndexFailureException e )
                {
                    throw new ConstraintViolationException( e.getUserMessage(
                            new StatementTokenNameLookup( statement.readOperations() ) ) );
                }
                catch ( InvalidTransactionTypeKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
            }
        }

        @Override
        public ConstraintDefinition createPropertyUniquenessConstraint( IndexDefinition indexDefinition )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int labelId = statement.tokenWriteOperations().labelGetOrCreateForName(
                            indexDefinition.getLabel().name() );
                    int[] propertyKeyIds = getOrCreatePropertyKeyIds(
                            statement.tokenWriteOperations(), indexDefinition );
                    statement.schemaWriteOperations().uniquePropertyConstraintCreate(
                            SchemaDescriptorFactory.forLabel( labelId, propertyKeyIds ) );
                    return new UniquenessConstraintDefinition( this, indexDefinition );
                }
                catch ( AlreadyConstrainedException | CreateConstraintFailureException | AlreadyIndexedException |
                        RepeatedPropertyInCompositeSchemaException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
                }
                catch ( IllegalTokenNameException e )
                {
                    throw new IllegalArgumentException( e );
                }
                catch ( TooManyLabelsException e )
                {
                    throw new IllegalStateException( e );
                }
                catch ( InvalidTransactionTypeKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
            }
        }

        @Override
        public ConstraintDefinition createNodeKeyConstraint( IndexDefinition indexDefinition )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int labelId = statement.tokenWriteOperations().labelGetOrCreateForName(
                            indexDefinition.getLabel().name() );
                    int[] propertyKeyIds = getOrCreatePropertyKeyIds(
                            statement.tokenWriteOperations(), indexDefinition );
                    statement.schemaWriteOperations().nodeKeyConstraintCreate(
                            SchemaDescriptorFactory.forLabel( labelId, propertyKeyIds ) );
                    return new NodeKeyConstraintDefinition( this, indexDefinition );
                }
                catch ( AlreadyConstrainedException | CreateConstraintFailureException | AlreadyIndexedException |
                        RepeatedPropertyInCompositeSchemaException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
                }
                catch ( IllegalTokenNameException e )
                {
                    throw new IllegalArgumentException( e );
                }
                catch ( TooManyLabelsException e )
                {
                    throw new IllegalStateException( e );
                }
                catch ( InvalidTransactionTypeKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
            }
        }

        @Override
        public ConstraintDefinition createPropertyExistenceConstraint( Label label, String... propertyKeys )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int labelId = statement.tokenWriteOperations().labelGetOrCreateForName( label.name() );
                    int[] propertyKeyIds = getOrCreatePropertyKeyIds(
                            statement.tokenWriteOperations(), propertyKeys );
                    statement.schemaWriteOperations().nodePropertyExistenceConstraintCreate(
                                    SchemaDescriptorFactory.forLabel( labelId, propertyKeyIds ) );
                    return new NodePropertyExistenceConstraintDefinition( this, label, propertyKeys );
                }
                catch ( AlreadyConstrainedException | CreateConstraintFailureException |
                        RepeatedPropertyInCompositeSchemaException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
                }
                catch ( IllegalTokenNameException e )
                {
                    throw new IllegalArgumentException( e );
                }
                catch ( TooManyLabelsException e )
                {
                    throw new IllegalStateException( e );
                }
                catch ( InvalidTransactionTypeKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
            }
        }

        @Override
        public ConstraintDefinition createPropertyExistenceConstraint( RelationshipType type, String propertyKey )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int typeId = statement.tokenWriteOperations().relationshipTypeGetOrCreateForName( type.name() );
                    int[] propertyKeyId = getOrCreatePropertyKeyIds( statement.tokenWriteOperations(), propertyKey );
                    statement.schemaWriteOperations().relationshipPropertyExistenceConstraintCreate(
                            SchemaDescriptorFactory.forRelType( typeId, propertyKeyId ) );
                    return new RelationshipPropertyExistenceConstraintDefinition( this, type, propertyKey );
                }
                catch ( AlreadyConstrainedException | CreateConstraintFailureException |
                        RepeatedPropertyInCompositeSchemaException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
                }
                catch ( IllegalTokenNameException e )
                {
                    throw new IllegalArgumentException( e );
                }
                catch ( InvalidTransactionTypeKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
            }
        }

        @Override
        public void dropPropertyUniquenessConstraint( Label label, String[] properties )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int labelId = statement.readOperations().labelGetForName( label.name() );
                    int[] propertyKeyIds = PropertyNameUtils.getPropertyIds( statement.readOperations(), properties );
                    statement.schemaWriteOperations().constraintDrop(
                            ConstraintDescriptorFactory.uniqueForLabel( labelId, propertyKeyIds ) );
                }
                catch ( DropConstraintFailureException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
                }
                catch ( InvalidTransactionTypeKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
            }
        }

        @Override
        public void dropNodeKeyConstraint( Label label, String[] properties )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int labelId = statement.readOperations().labelGetForName( label.name() );
                    int[] propertyKeyIds = PropertyNameUtils.getPropertyIds( statement.readOperations(), properties );
                    statement.schemaWriteOperations().constraintDrop(
                            ConstraintDescriptorFactory.nodeKeyForLabel( labelId, propertyKeyIds ) );
                }
                catch ( DropConstraintFailureException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
                }
                catch ( InvalidTransactionTypeKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
            }
        }

        @Override
        public void dropNodePropertyExistenceConstraint( Label label, String[] properties )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int labelId = statement.readOperations().labelGetForName( label.name() );
                    int[] propertyKeyIds = PropertyNameUtils.getPropertyIds( statement.readOperations(), properties );
                    statement.schemaWriteOperations().constraintDrop(
                            ConstraintDescriptorFactory.existsForLabel( labelId, propertyKeyIds ) );
                }
                catch ( DropConstraintFailureException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
                }
                catch ( InvalidTransactionTypeKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
            }
        }

        @Override
        public void dropRelationshipPropertyExistenceConstraint( RelationshipType type, String propertyKey )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                try
                {
                    int typeId = statement.readOperations().relationshipTypeGetForName( type.name() );
                    int propertyKeyId = statement.readOperations().propertyKeyGetForName( propertyKey );
                    statement.schemaWriteOperations().constraintDrop(
                            ConstraintDescriptorFactory.existsForRelType( typeId, propertyKeyId ) );
                }
                catch ( DropConstraintFailureException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) ), e );
                }
                catch ( InvalidTransactionTypeKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
            }
        }

        @Override
        public String getUserMessage( KernelException e )
        {
            try ( Statement statement = ctxSupplier.get() )
            {
                return e.getUserMessage( new StatementTokenNameLookup( statement.readOperations() ) );
            }
        }

        @Override
        public void assertInOpenTransaction()
        {
            ctxSupplier.get().close();
        }


    }
}
