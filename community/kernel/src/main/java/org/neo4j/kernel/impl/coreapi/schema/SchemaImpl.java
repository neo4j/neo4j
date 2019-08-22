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
package org.neo4j.kernel.impl.coreapi.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexPopulationProgress;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.schema.TokenCapacityExceededKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedPropertyInCompositeSchemaException;
import org.neo4j.kernel.impl.api.index.IndexPopulationFailure;

import static java.util.Collections.emptyList;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.graphdb.schema.Schema.IndexState.FAILED;
import static org.neo4j.graphdb.schema.Schema.IndexState.ONLINE;
import static org.neo4j.graphdb.schema.Schema.IndexState.POPULATING;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.helpers.collection.Iterators.addToCollection;
import static org.neo4j.internal.helpers.collection.Iterators.asCollection;
import static org.neo4j.internal.helpers.collection.Iterators.map;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.internal.schema.SchemaDescriptor.forRelType;
import static org.neo4j.internal.schema.SchemaDescriptor.fulltext;
import static org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl.labelNameList;
import static org.neo4j.kernel.impl.coreapi.schema.PropertyNameUtils.getOrCreatePropertyKeyIds;

public class SchemaImpl implements Schema
{
    private final Supplier<KernelTransaction> transactionSupplier;
    private final InternalSchemaActions actions;

    public SchemaImpl( Supplier<KernelTransaction> transactionSupplier )
    {
        this.transactionSupplier = transactionSupplier;
        this.actions = new GDBSchemaActions( transactionSupplier );
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
            Iterator<IndexDescriptor> indexes = schemaRead.indexesGetForLabel( labelId );
            addDefinitions( definitions, tokenRead, IndexDescriptor.sortByType( indexes ) );
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

            Iterator<IndexDescriptor> indexes = schemaRead.indexesGetAll();
            addDefinitions( definitions, transaction.tokenRead(), IndexDescriptor.sortByType( indexes ) );
            return definitions;
        }
    }

    private IndexDefinition descriptorToDefinition( final TokenRead tokenRead, IndexDescriptor index )
    {
        try
        {
            SchemaDescriptor schema = index.schema();
            int[] entityTokenIds = schema.getEntityTokenIds();
            boolean constraintIndex = index.isUnique();
            String[] propertyNames = PropertyNameUtils.getPropertyKeys( tokenRead, index.schema().getPropertyIds() );
            switch ( schema.entityType() )
            {
            case NODE:
                Label[] labels = new Label[entityTokenIds.length];
                for ( int i = 0; i < labels.length; i++ )
                {
                    labels[i] = label( tokenRead.nodeLabelName( entityTokenIds[i] ) );
                }
                return new IndexDefinitionImpl( actions, index, labels, propertyNames, constraintIndex );
            case RELATIONSHIP:
                RelationshipType[] relTypes = new RelationshipType[entityTokenIds.length];
                for ( int i = 0; i < relTypes.length; i++ )
                {
                    relTypes[i] = withName( tokenRead.relationshipTypeName( entityTokenIds[i] ) );
                }
                return new IndexDefinitionImpl( actions, index, relTypes, propertyNames, constraintIndex );
            default:
                throw new IllegalArgumentException( "Cannot create IndexDefinition for " + schema.entityType() + " entity-typed schema." );
            }
        }
        catch ( KernelException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void addDefinitions( List<IndexDefinition> definitions, final TokenRead tokenRead,
            Iterator<IndexDescriptor> indexes )
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
                String cause = getIndexFailure( index );
                String message = IndexPopulationFailure
                        .appendCauseOfFailure( String.format( "Index %s entered a %s state. Please see database logs.", index, state ), cause );
                throw new IllegalStateException( message );
            default:
                try
                {
                    Thread.sleep( 100 );
                }
                catch ( InterruptedException e )
                {
                    // Ignore interrupted exceptions here.
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
    public ConstraintDefinition getConstraintByName( String constraintName )
    {
        Objects.requireNonNull( constraintName );
        KernelTransaction transaction = safeAcquireTransaction( transactionSupplier );
        try ( Statement ignore = transaction.acquireStatement() )
        {
            ConstraintDescriptor constraint = transaction.schemaRead().constraintGetForName( constraintName );
            if ( constraint == null )
            {
                throw new IllegalArgumentException( "No constraint found with the name '" + constraintName + "'." );
            }
            return asConstraintDefinition( constraint, transaction.tokenRead() );
        }
    }

    @Override
    public IndexDefinition getIndexByName( String indexName )
    {
        Objects.requireNonNull( indexName );
        Iterator<IndexDefinition> indexes = getIndexes().iterator();
        IndexDefinition index = null;
        while ( indexes.hasNext() )
        {
            IndexDefinition candidate = indexes.next();
            if ( candidate.getName().equals( indexName ) )
            {
                if ( index != null )
                {
                    throw new IllegalStateException( "Multiple indexes found by the name '" + indexName + "'. " +
                            "Try iterating Schema#getIndexes() and filter by name instead." );
                }
                index = candidate;
            }
        }
        if ( index == null )
        {
            throw new IllegalArgumentException( "No index found with the name '" + indexName + "'." );
        }
        return index;
    }

    @Override
    public IndexState getIndexState( final IndexDefinition index )
    {
        KernelTransaction transaction = safeAcquireTransaction( transactionSupplier );
        try ( Statement ignore = transaction.acquireStatement() )
        {

            SchemaRead schemaRead = transaction.schemaRead();
            IndexDescriptor reference = getIndexReference( schemaRead, transaction.tokenRead(), (IndexDefinitionImpl) index );
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
            throw newIndexNotFoundException( index, e );
        }
    }

    private NotFoundException newIndexNotFoundException( IndexDefinition index, KernelException e )
    {
        return new NotFoundException( "No index was found corresponding to " + index + ".", e );
    }

    @Override
    public IndexPopulationProgress getIndexPopulationProgress( IndexDefinition index )
    {
        KernelTransaction transaction = safeAcquireTransaction( transactionSupplier );
        try ( Statement ignore = transaction.acquireStatement() )
        {
            SchemaRead schemaRead = transaction.schemaRead();
            IndexDescriptor descriptor = getIndexReference( schemaRead, transaction.tokenRead(), (IndexDefinitionImpl) index );
            PopulationProgress progress = schemaRead.indexGetPopulationProgress( descriptor );
            return progress.toIndexPopulationProgress();
        }
        catch ( SchemaRuleNotFoundException | IndexNotFoundKernelException e )
        {
            throw newIndexNotFoundException( index, e );
        }
    }

    @Override
    public String getIndexFailure( IndexDefinition index )
    {
        KernelTransaction transaction = safeAcquireTransaction( transactionSupplier );
        try ( Statement ignore = transaction.acquireStatement() )
        {
            SchemaRead schemaRead = transaction.schemaRead();
            IndexDescriptor descriptor = getIndexReference( schemaRead, transaction.tokenRead(), (IndexDefinitionImpl) index );
            return schemaRead.indexGetFailure( descriptor );
        }
        catch ( SchemaRuleNotFoundException | IndexNotFoundKernelException e )
        {
            throw newIndexNotFoundException( index, e );
        }
    }

    @Override
    public ConstraintCreator constraintFor( Label label )
    {
        actions.assertInOpenTransaction();
        return new BaseNodeConstraintCreator( actions, null, label );
    }

    @Override
    public ConstraintCreator constraintFor( RelationshipType type )
    {
        actions.assertInOpenTransaction();
        return new BaseRelationshipConstraintCreator( actions, null, type );
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints()
    {
        KernelTransaction transaction = safeAcquireTransaction( transactionSupplier );
        try ( Statement ignore = transaction.acquireStatement() )
        {
            return asConstraintDefinitions( transaction.schemaRead().constraintsGetAll(), transaction.tokenRead() );
        }
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints( final Label label )
    {
        KernelTransaction transaction = safeAcquireTransaction( transactionSupplier );
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
        KernelTransaction transaction = safeAcquireTransaction( transactionSupplier );
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

    private static IndexDescriptor getIndexReference( SchemaRead schemaRead, TokenRead tokenRead, IndexDefinitionImpl index )
            throws SchemaRuleNotFoundException
    {
        // Use the precise embedded index reference when available.
        IndexDescriptor reference = index.getIndexReference();
        if ( reference != null )
        {
            return reference;
        }

        // Otherwise attempt to reverse engineer the schema that will let us look up the real IndexReference.
        int[] propertyKeyIds = resolveAndValidatePropertyKeys( tokenRead, index.getPropertyKeysArrayShared() );
        SchemaDescriptor schema;

        if ( index.isNodeIndex() )
        {
            int[] labelIds = resolveAndValidateTokens( "Label", index.getLabelArrayShared(), Label::name, tokenRead::nodeLabel );

            if ( index.isMultiTokenIndex() )
            {
                schema = fulltext( EntityType.NODE, IndexConfig.empty(), labelIds, propertyKeyIds );
            }
            else
            {
                schema = forLabel( labelIds[0], propertyKeyIds );
            }
        }
        else if ( index.isRelationshipIndex() )
        {
            int[] relTypes = resolveAndValidateTokens(
                    "Relationship type", index.getRelationshipTypesArrayShared(), RelationshipType::name, tokenRead::relationshipType );

            if ( index.isMultiTokenIndex() )
            {
                schema = fulltext( EntityType.RELATIONSHIP, IndexConfig.empty(), relTypes, propertyKeyIds );
            }
            else
            {
                schema = forRelType( relTypes[0], propertyKeyIds );
            }
        }
        else
        {
            throw new IllegalArgumentException( "The given index is neither a node index, nor a relationship index: " + index + "." );
        }

        reference = schemaRead.index( schema );
        if ( reference == IndexDescriptor.NO_INDEX )
        {
            throw new SchemaRuleNotFoundException( schema );
        }

        return reference;
    }

    private static int[] resolveAndValidatePropertyKeys( TokenRead tokenRead, String[] propertyKeys )
    {
        return resolveAndValidateTokens( "Property key", propertyKeys, s -> s, tokenRead::propertyKey );
    }

    private static <T> int[] resolveAndValidateTokens( String tokenTypeName, T[] tokens, Function<T,String> getTokenName, ToIntFunction<String> getTokenId )
    {
        int[] tokenIds = new int[tokens.length];
        for ( int i = 0; i < tokenIds.length; i++ )
        {
            String tokenName = getTokenName.apply( tokens[i] );
            int tokenId = getTokenId.applyAsInt( tokenName );
            if ( tokenId == TokenRead.NO_TOKEN )
            {
                throw new NotFoundException( tokenTypeName + " " + tokenName + " not found." );
            }
            tokenIds[i] = tokenId;
        }
        return tokenIds;
    }

    private Iterable<ConstraintDefinition> asConstraintDefinitions(
            Iterator<? extends ConstraintDescriptor> constraints,
            TokenRead tokenRead )
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

    private ConstraintDefinition asConstraintDefinition( ConstraintDescriptor constraint, TokenRead tokenRead )
    {
        // This was turned inside out. Previously a low-level constraint object would reference a public enum type
        // which made it impossible to break out the low-level component from kernel. There could be a lower level
        // constraint type introduced to mimic the public ConstraintType, but that would be a duplicate of it
        // essentially. Checking instanceof here is OK-ish since the objects it checks here are part of the
        // internal storage engine API.
        SilentTokenNameLookup lookup = new SilentTokenNameLookup( tokenRead );
        if ( constraint.isNodePropertyExistenceConstraint() ||
             constraint.isNodeKeyConstraint() ||
             constraint.isUniquenessConstraint() )
        {
            SchemaDescriptor schemaDescriptor = constraint.schema();
            int[] entityTokenIds = schemaDescriptor.getEntityTokenIds();
            Label[] labels = new Label[entityTokenIds.length];
            for ( int i = 0; i < entityTokenIds.length; i++ )
            {
                labels[i] = label( lookup.labelGetName( entityTokenIds[i] ) );
            }
            String[] propertyKeys = Arrays.stream( schemaDescriptor.getPropertyIds() ).mapToObj( lookup::propertyKeyGetName ).toArray( String[]::new );
            if ( constraint.isNodePropertyExistenceConstraint() )
            {
                return new NodePropertyExistenceConstraintDefinition( actions, constraint, labels[0], propertyKeys );
            }
            else if ( constraint.isUniquenessConstraint() )
            {
                return new UniquenessConstraintDefinition( actions, constraint, new IndexDefinitionImpl( actions, null, labels, propertyKeys, true ) );
            }
            else
            {
                return new NodeKeyConstraintDefinition( actions, constraint, new IndexDefinitionImpl( actions, null, labels, propertyKeys, true ) );
            }
        }
        else if ( constraint.isRelationshipPropertyExistenceConstraint() )
        {
            RelationTypeSchemaDescriptor descriptor = constraint.schema().asRelationshipTypeSchemaDescriptor();
            return new RelationshipPropertyExistenceConstraintDefinition( actions, constraint,
                    withName( lookup.relationshipTypeGetName( descriptor.getRelTypeId() ) ),
                    lookup.propertyKeyGetName( descriptor.getPropertyId() ) );
        }
        throw new IllegalArgumentException( "Unknown constraint " + constraint );
    }

    private static KernelTransaction safeAcquireTransaction( Supplier<KernelTransaction> transactionSupplier )
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
        private final Supplier<KernelTransaction> transactionSupplier;

        GDBSchemaActions( Supplier<KernelTransaction> transactionSupplier )
        {
            this.transactionSupplier = transactionSupplier;
        }

        @Override
        public IndexDefinition createIndexDefinition( Label label, String indexName, String... propertyKeys )
        {
            KernelTransaction transaction = safeAcquireTransaction( transactionSupplier );

            try ( Statement ignore = transaction.acquireStatement() )
            {
                try
                {
                    TokenWrite tokenWrite = transaction.tokenWrite();
                    int labelId = tokenWrite.labelGetOrCreateForName( label.name() );
                    int[] propertyKeyIds = getOrCreatePropertyKeyIds( tokenWrite, propertyKeys );
                    LabelSchemaDescriptor descriptor = forLabel( labelId, propertyKeyIds );
                    IndexDescriptor indexReference = transaction.schemaWrite().indexCreate( descriptor, indexName );
                    return new IndexDefinitionImpl( this, indexReference, new Label[]{label}, propertyKeys, false );
                }
                catch ( IllegalTokenNameException e )
                {
                    throw new IllegalArgumentException( e );
                }
                catch ( InvalidTransactionTypeKernelException | SchemaKernelException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new SilentTokenNameLookup( transaction.tokenRead() ) ), e );
                }
                catch ( KernelException e )
                {
                    throw new TransactionFailureException( "Unknown error trying to create token ids", e );
                }
            }
        }

        @Override
        public void dropIndexDefinitions( IndexDefinition indexDefinition )
        {
            KernelTransaction transaction = safeAcquireTransaction( transactionSupplier );
            try ( Statement ignore = transaction.acquireStatement() )
            {
                try
                {
                    IndexDescriptor reference = getIndexReference( transaction.schemaRead(), transaction.tokenRead(), (IndexDefinitionImpl) indexDefinition );
                    transaction.schemaWrite().indexDrop( reference );
                }
                catch ( NotFoundException e )
                {
                    // Silently ignore invalid label and property names
                }
                catch ( SchemaRuleNotFoundException | DropIndexFailureException e )
                {
                    throw new ConstraintViolationException( e.getUserMessage(
                            new SilentTokenNameLookup( transaction.tokenRead() ) ), e );
                }
                catch ( InvalidTransactionTypeKernelException | SchemaKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
            }
        }

        @Override
        public ConstraintDefinition createPropertyUniquenessConstraint( IndexDefinition indexDefinition, String name )
        {
            if ( indexDefinition.isMultiTokenIndex() )
            {
                throw new ConstraintViolationException( "A property uniqueness constraint does not support multi-token index definitions. " +
                        "That is, only a single label is supported, but the following labels were provided: " +
                        labelNameList( indexDefinition.getLabels(), "", "." ) );
            }
            KernelTransaction transaction = safeAcquireTransaction( transactionSupplier );
            try ( Statement ignore = transaction.acquireStatement() )
            {
                try
                {
                    TokenWrite tokenWrite = transaction.tokenWrite();
                    int labelId = tokenWrite.labelGetOrCreateForName( single( indexDefinition.getLabels() ).name() );
                    int[] propertyKeyIds = getOrCreatePropertyKeyIds( tokenWrite, indexDefinition );
                    LabelSchemaDescriptor schema = forLabel( labelId, propertyKeyIds );
                    ConstraintDescriptor constraint = transaction.schemaWrite().uniquePropertyConstraintCreate( schema, name );
                    return new UniquenessConstraintDefinition( this, constraint, indexDefinition );
                }
                catch ( AlreadyConstrainedException | CreateConstraintFailureException | AlreadyIndexedException |
                        RepeatedPropertyInCompositeSchemaException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new SilentTokenNameLookup( transaction.tokenRead() ) ), e );
                }
                catch ( IllegalTokenNameException e )
                {
                    throw new IllegalArgumentException( e );
                }
                catch ( TokenCapacityExceededKernelException e )
                {
                    throw new IllegalStateException( e );
                }
                catch ( InvalidTransactionTypeKernelException | SchemaKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
                catch ( KernelException e )
                {
                    throw new TransactionFailureException( "Unknown error trying to create token ids", e );
                }
            }
        }

        @Override
        public ConstraintDefinition createNodeKeyConstraint( IndexDefinition indexDefinition, String name )
        {
            if ( indexDefinition.isMultiTokenIndex() )
            {
                throw new ConstraintViolationException( "A node key constraint does not support multi-token index definitions. " +
                        "That is, only a single label is supported, but the following labels were provided: " +
                        labelNameList( indexDefinition.getLabels(), "", "." ) );
            }
            KernelTransaction transaction = safeAcquireTransaction( transactionSupplier );
            try ( Statement ignore = transaction.acquireStatement() )
            {
                try
                {
                    TokenWrite tokenWrite = transaction.tokenWrite();
                    int labelId = tokenWrite.labelGetOrCreateForName( single( indexDefinition.getLabels() ).name() );
                    int[] propertyKeyIds = getOrCreatePropertyKeyIds( tokenWrite, indexDefinition );
                    LabelSchemaDescriptor schema = forLabel( labelId, propertyKeyIds );
                    ConstraintDescriptor constraint = transaction.schemaWrite().nodeKeyConstraintCreate( schema, name );
                    return new NodeKeyConstraintDefinition( this, constraint, indexDefinition );
                }
                catch ( AlreadyConstrainedException | CreateConstraintFailureException | AlreadyIndexedException |
                        RepeatedPropertyInCompositeSchemaException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new SilentTokenNameLookup( transaction.tokenRead() ) ), e );
                }
                catch ( IllegalTokenNameException e )
                {
                    throw new IllegalArgumentException( e );
                }
                catch ( TokenCapacityExceededKernelException e )
                {
                    throw new IllegalStateException( e );
                }
                catch ( InvalidTransactionTypeKernelException | SchemaKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
                catch ( KernelException e )
                {
                    throw new TransactionFailureException( "Unknown error trying to create token ids", e );
                }
            }
        }

        @Override
        public ConstraintDefinition createPropertyExistenceConstraint( String name, Label label, String... propertyKeys )
        {
            KernelTransaction transaction = safeAcquireTransaction( transactionSupplier );
            try ( Statement ignore = transaction.acquireStatement() )
            {
                try
                {
                    TokenWrite tokenWrite = transaction.tokenWrite();
                    int labelId = tokenWrite.labelGetOrCreateForName( label.name() );
                    int[] propertyKeyIds = getOrCreatePropertyKeyIds( tokenWrite, propertyKeys );
                    LabelSchemaDescriptor schema = forLabel( labelId, propertyKeyIds );
                    ConstraintDescriptor constraint = transaction.schemaWrite().nodePropertyExistenceConstraintCreate( schema, name );
                    return new NodePropertyExistenceConstraintDefinition( this, constraint, label, propertyKeys );
                }
                catch ( AlreadyConstrainedException | CreateConstraintFailureException |
                        RepeatedPropertyInCompositeSchemaException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new SilentTokenNameLookup( transaction.tokenRead() ) ), e );
                }
                catch ( IllegalTokenNameException e )
                {
                    throw new IllegalArgumentException( e );
                }
                catch ( TokenCapacityExceededKernelException e )
                {
                    throw new IllegalStateException( e );
                }
                catch ( InvalidTransactionTypeKernelException | SchemaKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
                catch ( KernelException e )
                {
                    throw new TransactionFailureException( "Unknown error trying to create token ids", e );
                }
            }
        }

        @Override
        public ConstraintDefinition createPropertyExistenceConstraint( String name, RelationshipType type, String propertyKey )
        {
            KernelTransaction transaction = safeAcquireTransaction( transactionSupplier );
            try ( Statement ignore = transaction.acquireStatement() )
            {
                try
                {
                    TokenWrite tokenWrite = transaction.tokenWrite();
                    int typeId = tokenWrite.relationshipTypeGetOrCreateForName( type.name() );
                    int[] propertyKeyId = getOrCreatePropertyKeyIds( tokenWrite, propertyKey );
                    RelationTypeSchemaDescriptor schema = forRelType( typeId, propertyKeyId );
                    ConstraintDescriptor constraint = transaction.schemaWrite().relationshipPropertyExistenceConstraintCreate( schema, name );
                    return new RelationshipPropertyExistenceConstraintDefinition( this, constraint, type, propertyKey );
                }
                catch ( AlreadyConstrainedException | CreateConstraintFailureException |
                        RepeatedPropertyInCompositeSchemaException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new SilentTokenNameLookup( transaction.tokenRead() ) ), e );
                }
                catch ( IllegalTokenNameException e )
                {
                    throw new IllegalArgumentException( e );
                }
                catch ( InvalidTransactionTypeKernelException | SchemaKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
                catch ( KernelException e )
                {
                    throw new TransactionFailureException( "Unknown error trying to create token ids", e );
                }
            }
        }

        @Override
        public void dropConstraint( ConstraintDescriptor constraint )
        {
            KernelTransaction transaction = safeAcquireTransaction( transactionSupplier );
            try ( Statement ignore = transaction.acquireStatement() )
            {
                try
                {
                    transaction.schemaWrite().constraintDrop( constraint );
                }
                catch ( DropConstraintFailureException e )
                {
                    throw new ConstraintViolationException(
                            e.getUserMessage( new SilentTokenNameLookup( transaction.tokenRead() ) ), e );
                }
                catch ( InvalidTransactionTypeKernelException | SchemaKernelException e )
                {
                    throw new ConstraintViolationException( e.getMessage(), e );
                }
            }
        }

        @Override
        public String getUserMessage( KernelException e )
        {
            KernelTransaction transaction = safeAcquireTransaction( transactionSupplier );
            try ( Statement ignore = transaction.acquireStatement() )
            {
                return e.getUserMessage( new SilentTokenNameLookup( transaction.tokenRead() ) );
            }
        }

        @Override
        public void assertInOpenTransaction()
        {
            KernelTransaction transaction = transactionSupplier.get();
            if ( transaction.isTerminated() )
            {
                Status terminationReason = transaction.getReasonIfTerminated().orElse( Status.Transaction.Terminated );
                throw new TransactionTerminatedException( terminationReason );
            }
        }
    }
}
