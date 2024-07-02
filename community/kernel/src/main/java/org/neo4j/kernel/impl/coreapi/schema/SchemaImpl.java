/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.coreapi.schema;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.graphdb.schema.Schema.IndexState.FAILED;
import static org.neo4j.graphdb.schema.Schema.IndexState.ONLINE;
import static org.neo4j.graphdb.schema.Schema.IndexState.POPULATING;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.internal.helpers.collection.Iterators.addToCollection;
import static org.neo4j.internal.helpers.collection.Iterators.map;
import static org.neo4j.internal.schema.IndexType.fromPublicApi;
import static org.neo4j.internal.schema.SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR;
import static org.neo4j.internal.schema.SchemaDescriptors.ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.internal.schema.SchemaDescriptors.forRelType;
import static org.neo4j.internal.schema.SchemaDescriptors.fulltext;
import static org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl.labelNameList;
import static org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl.relTypeNameList;
import static org.neo4j.kernel.impl.coreapi.schema.PropertyNameUtils.getOrCreatePropertyKeyIds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.schema.AnyTokens;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexPopulationProgress;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.helpers.collection.Iterators;
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
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaRuleException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.schema.TokenCapacityExceededKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.ConflictingConstraintException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.RepeatedSchemaComponentException;
import org.neo4j.kernel.impl.api.index.IndexPopulationFailure;
import org.neo4j.time.Stopwatch;

public class SchemaImpl implements Schema {
    private final InternalSchemaActions actions;
    private final KernelTransaction transaction;

    public SchemaImpl(KernelTransaction transaction) {
        this.transaction = transaction;
        this.actions = new GDBSchemaActions(transaction);
    }

    @Override
    public IndexCreator indexFor(Label label) {
        return new IndexCreatorImpl(actions, label);
    }

    @Override
    public IndexCreator indexFor(Label... labels) {
        return new IndexCreatorImpl(actions, labels);
    }

    @Override
    public IndexCreator indexFor(RelationshipType type) {
        return new IndexCreatorImpl(actions, type);
    }

    @Override
    public IndexCreator indexFor(RelationshipType... types) {
        return new IndexCreatorImpl(actions, types);
    }

    @Override
    public IndexCreator indexFor(AnyTokens tokens) {
        return new TokenIndexCreator(actions, tokens);
    }

    @Override
    public Iterable<IndexDefinition> getIndexes(final Label label) {
        transaction.assertOpen();
        TokenRead tokenRead = transaction.tokenRead();
        SchemaRead schemaRead = transaction.schemaRead();
        List<IndexDefinition> definitions = new ArrayList<>();
        int labelId = tokenRead.nodeLabel(label.name());
        if (labelId == TokenRead.NO_TOKEN) {
            return emptyList();
        }
        Iterator<IndexDescriptor> indexes = schemaRead.indexesGetForLabel(labelId);
        addDefinitions(definitions, tokenRead, IndexDescriptor.sortByType(indexes));
        return definitions;
    }

    @Override
    public Iterable<IndexDefinition> getIndexes(RelationshipType relationshipType) {
        transaction.assertOpen();
        TokenRead tokenRead = transaction.tokenRead();
        SchemaRead schemaRead = transaction.schemaRead();
        List<IndexDefinition> definitions = new ArrayList<>();
        int relationshipTypeId = tokenRead.relationshipType(relationshipType.name());
        if (relationshipTypeId == TokenRead.NO_TOKEN) {
            return emptyList();
        }
        Iterator<IndexDescriptor> indexes = schemaRead.indexesGetForRelationshipType(relationshipTypeId);
        addDefinitions(definitions, tokenRead, IndexDescriptor.sortByType(indexes));
        return definitions;
    }

    @Override
    public Iterable<IndexDefinition> getIndexes() {
        transaction.assertOpen();
        SchemaRead schemaRead = transaction.schemaRead();
        List<IndexDefinition> definitions = new ArrayList<>();

        Iterator<IndexDescriptor> indexes = schemaRead.indexesGetAll();
        addDefinitions(definitions, transaction.tokenRead(), IndexDescriptor.sortByType(indexes));
        return definitions;
    }

    private IndexDefinition descriptorToDefinition(final TokenRead tokenRead, IndexDescriptor index) {
        try {
            SchemaDescriptor schema = index.schema();
            int[] entityTokenIds = schema.getEntityTokenIds();
            boolean constraintIndex = index.isUnique();
            String[] propertyNames = PropertyNameUtils.getPropertyKeysOrThrow(
                    tokenRead, index.schema().getPropertyIds());
            switch (schema.entityType()) {
                case NODE:
                    Label[] labels = new Label[entityTokenIds.length];
                    for (int i = 0; i < labels.length; i++) {
                        labels[i] = label(tokenRead.nodeLabelName(entityTokenIds[i]));
                    }
                    return new IndexDefinitionImpl(actions, index, labels, propertyNames, constraintIndex);
                case RELATIONSHIP:
                    RelationshipType[] relTypes = new RelationshipType[entityTokenIds.length];
                    for (int i = 0; i < relTypes.length; i++) {
                        relTypes[i] = withName(tokenRead.relationshipTypeName(entityTokenIds[i]));
                    }
                    return new IndexDefinitionImpl(actions, index, relTypes, propertyNames, constraintIndex);
                default:
                    throw new IllegalArgumentException(
                            "Cannot create IndexDefinition for " + schema.entityType() + " entity-typed schema.");
            }
        } catch (KernelException e) {
            throw new RuntimeException(e);
        }
    }

    private void addDefinitions(
            List<IndexDefinition> definitions, final TokenRead tokenRead, Iterator<IndexDescriptor> indexes) {
        addToCollection(map(index -> descriptorToDefinition(tokenRead, index), indexes), definitions);
    }

    @Override
    public void awaitIndexOnline(IndexDefinition index, long duration, TimeUnit unit) {
        actions.assertInOpenTransaction();

        IndexDescriptor reference = ((IndexDefinitionImpl) index).getIndexReference();
        Iterable<IndexDescriptor> iterable = () -> Iterators.iterator(reference);
        if (awaitIndexesOnline(iterable, descriptor -> index.toString(), duration, unit, true)) {
            throw new IllegalStateException("Expected index to come online within a reasonable time.");
        }
    }

    @Override
    public void awaitIndexOnline(String indexName, long duration, TimeUnit unit) {
        requireNonNull(indexName);
        transaction.assertOpen();

        SchemaRead schemaRead = transaction.schemaRead();
        Iterable<IndexDescriptor> iterable = () -> Iterators.iterator(schemaRead.indexGetForName(indexName));
        if (awaitIndexesOnline(iterable, index -> "`" + indexName + "`", duration, unit, false)) {
            throw new IllegalStateException("Expected index to come online within a reasonable time.");
        }
    }

    @Override
    public void awaitIndexesOnline(long duration, TimeUnit unit) {
        transaction.assertOpen();

        Iterable<IndexDescriptor> iterable = () -> Iterators.map(
                def -> ((IndexDefinitionImpl) def).getIndexReference(),
                getIndexes().iterator());
        if (awaitIndexesOnline(
                iterable,
                index -> descriptorToDefinition(transaction.tokenRead(), index).toString(),
                duration,
                unit,
                false)) {
            List<IndexDefinition> online = new ArrayList<>();
            List<IndexDefinition> notOnline = new ArrayList<>();
            for (IndexDefinition index : getIndexes()) {
                try {
                    if (getIndexState(index) == ONLINE) {
                        online.add(index);
                        continue;
                    }
                } catch (Exception ignore) {
                }
                notOnline.add(index);
            }
            throw new IllegalStateException("Expected all indexes to come online within a reasonable time. "
                    + "Indexes brought online: " + online
                    + ". Indexes not guaranteed to be online: " + notOnline);
        }
    }

    private boolean awaitIndexesOnline(
            Iterable<IndexDescriptor> indexes,
            Function<IndexDescriptor, String> describe,
            long duration,
            TimeUnit unit,
            boolean bubbleNotFound) {
        Stopwatch startTime = Stopwatch.start();

        do {
            boolean allOnline = true;
            SchemaRead schemaRead = transaction.schemaRead();
            for (IndexDescriptor index : indexes) {
                if (index == IndexDescriptor.NO_INDEX) {
                    allOnline = false;
                    break;
                }

                try {
                    InternalIndexState indexState = schemaRead.indexGetState(index);
                    if (indexState == InternalIndexState.POPULATING) {
                        allOnline = false;
                        break;
                    }
                    if (indexState == InternalIndexState.FAILED) {
                        String cause = schemaRead.indexGetFailure(index);
                        String message = "Index " + describe.apply(index) + " entered a " + indexState
                                + " state. Please see database logs.";
                        message = IndexPopulationFailure.appendCauseOfFailure(message, cause);
                        throw new IllegalStateException(message);
                    }
                } catch (IndexNotFoundKernelException e) {
                    if (bubbleNotFound) {
                        throw newIndexNotFoundException(descriptorToDefinition(transaction.tokenRead(), index), e);
                    }
                    // Weird that the index vanished, but we'll just wait and see if it comes back until we time out.
                    allOnline = false;
                    break;
                }
            }

            if (allOnline) {
                return false;
            }
            sleepIgnoreInterrupt();
        } while (!startTime.hasTimedOut(duration, unit));

        return true;
    }

    private static void sleepIgnoreInterrupt() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // Ignore interrupted exceptions here.
        }
    }

    @Override
    public ConstraintDefinition getConstraintByName(String constraintName) {
        transaction.assertOpen();
        requireNonNull(constraintName);
        ConstraintDescriptor constraint = transaction.schemaRead().constraintGetForName(constraintName);
        if (constraint == null) {
            throw new IllegalArgumentException("No constraint found with the name '" + constraintName + "'.");
        }
        return asConstraintDefinition(constraint, transaction.tokenRead());
    }

    @Override
    public IndexDefinition getIndexByName(String indexName) {
        requireNonNull(indexName);
        transaction.assertOpen();
        var index = transaction.schemaRead().indexGetForName(indexName);
        if (index == IndexDescriptor.NO_INDEX) {
            throw new IllegalArgumentException("No index found with the name '" + indexName + "'.");
        }
        return descriptorToDefinition(transaction.tokenRead(), index);
    }

    @Override
    public IndexState getIndexState(final IndexDefinition index) {
        try {
            transaction.assertOpen();
            SchemaRead schemaRead = transaction.schemaRead();
            IndexDescriptor reference =
                    getIndexReference(schemaRead, transaction.tokenRead(), (IndexDefinitionImpl) index);
            InternalIndexState indexState = schemaRead.indexGetState(reference);
            return switch (indexState) {
                case POPULATING -> POPULATING;
                case ONLINE -> ONLINE;
                case FAILED -> FAILED;
            };
        } catch (KernelException e) {
            throw newIndexNotFoundException(index, e);
        }
    }

    private static NotFoundException newIndexNotFoundException(IndexDefinition index, KernelException e) {
        return new NotFoundException("No index was found corresponding to " + index + ".", e);
    }

    @Override
    public IndexPopulationProgress getIndexPopulationProgress(IndexDefinition index) {
        try {
            transaction.assertOpen();
            SchemaRead schemaRead = transaction.schemaRead();
            IndexDescriptor descriptor =
                    getIndexReference(schemaRead, transaction.tokenRead(), (IndexDefinitionImpl) index);
            PopulationProgress progress = schemaRead.indexGetPopulationProgress(descriptor);
            return progress.toIndexPopulationProgress();
        } catch (KernelException e) {
            throw newIndexNotFoundException(index, e);
        }
    }

    @Override
    public String getIndexFailure(IndexDefinition index) {
        try {
            transaction.assertOpen();
            SchemaRead schemaRead = transaction.schemaRead();
            IndexDescriptor descriptor =
                    getIndexReference(schemaRead, transaction.tokenRead(), (IndexDefinitionImpl) index);
            return schemaRead.indexGetFailure(descriptor);
        } catch (KernelException e) {
            throw newIndexNotFoundException(index, e);
        }
    }

    @Override
    public ConstraintCreator constraintFor(Label label) {
        transaction.assertOpen();
        return new BaseNodeConstraintCreator(actions, null, label, null, null);
    }

    @Override
    public ConstraintCreator constraintFor(RelationshipType type) {
        transaction.assertOpen();
        return new BaseRelationshipConstraintCreator(actions, null, type, null, null);
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints() {
        transaction.assertOpen();
        return asConstraintDefinitions(transaction.schemaRead().constraintsGetAll(), transaction.tokenRead());
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints(final Label label) {
        transaction.assertOpen();
        TokenRead tokenRead = transaction.tokenRead();
        SchemaRead schemaRead = transaction.schemaRead();
        int labelId = tokenRead.nodeLabel(label.name());
        if (labelId == TokenRead.NO_TOKEN) {
            return emptyList();
        }
        return asConstraintDefinitions(schemaRead.constraintsGetForLabel(labelId), tokenRead);
    }

    @Override
    public Iterable<ConstraintDefinition> getConstraints(RelationshipType type) {
        transaction.assertOpen();
        TokenRead tokenRead = transaction.tokenRead();
        SchemaRead schemaRead = transaction.schemaRead();
        int typeId = tokenRead.relationshipType(type.name());
        if (typeId == TokenRead.NO_TOKEN) {
            return emptyList();
        }
        return asConstraintDefinitions(schemaRead.constraintsGetForRelationshipType(typeId), tokenRead);
    }

    private static IndexDescriptor getIndexReference(
            SchemaRead schemaRead, TokenRead tokenRead, IndexDefinitionImpl index) throws SchemaRuleException {
        // Use the precise embedded index reference when available.
        IndexDescriptor reference = index.getIndexReference();
        if (reference != null) {
            return reference;
        }

        // Otherwise attempt to reverse engineer the schema that will let us look up the real IndexReference.
        int[] propertyKeyIds = resolveAndValidatePropertyKeys(tokenRead, index.getPropertyKeysArrayShared());
        SchemaDescriptor schema;

        if (index.isNodeIndex()) {
            int[] labelIds =
                    resolveAndValidateTokens("Label", index.getLabelArrayShared(), Label::name, tokenRead::nodeLabel);

            if (index.isMultiTokenIndex()) {
                schema = fulltext(EntityType.NODE, labelIds, propertyKeyIds);
            } else if (index.getIndexType() == IndexType.LOOKUP) {
                schema = ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR;
            } else {
                schema = forLabel(labelIds[0], propertyKeyIds);
            }
        } else if (index.isRelationshipIndex()) {
            int[] relTypes = resolveAndValidateTokens(
                    "Relationship type",
                    index.getRelationshipTypesArrayShared(),
                    RelationshipType::name,
                    tokenRead::relationshipType);

            if (index.isMultiTokenIndex()) {
                schema = fulltext(EntityType.RELATIONSHIP, relTypes, propertyKeyIds);
            } else if (index.getIndexType() == IndexType.LOOKUP) {
                schema = ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR;
            } else {
                schema = forRelType(relTypes[0], propertyKeyIds);
            }
        } else {
            throw new IllegalArgumentException(
                    "The given index is neither a node index, nor a relationship index: " + index + ".");
        }

        var foundReference = schemaRead.index(schema, fromPublicApi(index.getIndexType()));
        if (foundReference == IndexDescriptor.NO_INDEX) {
            throw new SchemaRuleNotFoundException(schema, tokenRead);
        }
        return foundReference;
    }

    private static int[] resolveAndValidatePropertyKeys(TokenRead tokenRead, String[] propertyKeys) {
        return resolveAndValidateTokens("Property key", propertyKeys, s -> s, tokenRead::propertyKey);
    }

    private static <T> int[] resolveAndValidateTokens(
            String tokenTypeName, T[] tokens, Function<T, String> getTokenName, ToIntFunction<String> getTokenId) {
        int[] tokenIds = new int[tokens.length];
        for (int i = 0; i < tokenIds.length; i++) {
            String tokenName = getTokenName.apply(tokens[i]);
            int tokenId = getTokenId.applyAsInt(tokenName);
            if (tokenId == TokenRead.NO_TOKEN) {
                throw new NotFoundException(tokenTypeName + " " + tokenName + " not found.");
            }
            tokenIds[i] = tokenId;
        }
        return tokenIds;
    }

    private Iterable<ConstraintDefinition> asConstraintDefinitions(
            Iterator<? extends ConstraintDescriptor> constraints, TokenRead tokenRead) {
        // Intentionally create an eager list so that used statement can be closed
        List<ConstraintDefinition> definitions = new ArrayList<>();

        while (constraints.hasNext()) {
            ConstraintDescriptor constraint = constraints.next();
            definitions.add(asConstraintDefinition(constraint, tokenRead));
        }

        return definitions;
    }

    private ConstraintDefinition asConstraintDefinition(ConstraintDescriptor constraint, TokenRead tokenRead) {
        // This was turned inside out. Previously a low-level constraint object would reference a public enum type
        // which made it impossible to break out the low-level component from kernel. There could be a lower level
        // constraint type introduced to mimic the public ConstraintType, but that would be a duplicate of it
        // essentially. Checking instanceof here is OK-ish since the objects it checks here are part of the
        // internal storage engine API.
        if (constraint.schema().isSchemaDescriptorType(LabelSchemaDescriptor.class)) {
            SchemaDescriptor schemaDescriptor = constraint.schema();
            int[] entityTokenIds = schemaDescriptor.getEntityTokenIds();
            Label[] labels = new Label[entityTokenIds.length];
            for (int i = 0; i < entityTokenIds.length; i++) {
                labels[i] = label(tokenRead.labelGetName(entityTokenIds[i]));
            }
            if (constraint.isNodePropertyTypeConstraint()) {
                return new NodePropertyTypeConstraintDefinition(
                        actions, constraint, labels[0], tokenRead.propertyKeyGetName(schemaDescriptor.getPropertyId()));
            }

            String[] propertyKeys = Arrays.stream(schemaDescriptor.getPropertyIds())
                    .mapToObj(tokenRead::propertyKeyGetName)
                    .toArray(String[]::new);
            if (constraint.isNodePropertyExistenceConstraint()) {
                return new NodePropertyExistenceConstraintDefinition(actions, constraint, labels[0], propertyKeys);
            } else if (constraint.isNodeUniquenessConstraint()) {
                return new NodeUniquenessConstraintDefinition(
                        actions, constraint, new IndexDefinitionImpl(actions, null, labels, propertyKeys, true));
            } else if (constraint.isNodeKeyConstraint()) {
                return new NodeKeyConstraintDefinition(
                        actions, constraint, new IndexDefinitionImpl(actions, null, labels, propertyKeys, true));
            }
        } else if (constraint.schema().isRelationshipTypeSchemaDescriptor()) {
            RelationTypeSchemaDescriptor descriptor = constraint.schema().asRelationshipTypeSchemaDescriptor();
            RelationshipType relationshipType = withName(tokenRead.relationshipTypeGetName(descriptor.getRelTypeId()));
            if (constraint.isRelationshipPropertyExistenceConstraint()) {
                return new RelationshipPropertyExistenceConstraintDefinition(
                        actions,
                        constraint,
                        relationshipType,
                        tokenRead.propertyKeyGetName(descriptor.getPropertyId()));
            } else if (constraint.isRelationshipPropertyTypeConstraint()) {
                return new RelationshipPropertyTypeConstraintDefinition(
                        actions,
                        constraint,
                        relationshipType,
                        tokenRead.propertyKeyGetName(descriptor.getPropertyId()));
            }
            String[] propertyKeys = Arrays.stream(descriptor.getPropertyIds())
                    .mapToObj(tokenRead::propertyKeyGetName)
                    .toArray(String[]::new);
            if (constraint.isRelationshipKeyConstraint()) {
                return new RelationshipKeyConstraintDefinition(
                        actions,
                        constraint,
                        new IndexDefinitionImpl(
                                actions, null, new RelationshipType[] {relationshipType}, propertyKeys, true));
            } else if (constraint.isRelationshipUniquenessConstraint()) {
                return new RelationshipUniquenessConstraintDefinition(
                        actions,
                        constraint,
                        new IndexDefinitionImpl(
                                actions, null, new RelationshipType[] {relationshipType}, propertyKeys, true));
            }
        }
        throw new IllegalArgumentException("Unknown constraint " + constraint);
    }

    private static class GDBSchemaActions implements InternalSchemaActions {
        private final KernelTransaction transaction;

        GDBSchemaActions(KernelTransaction transaction) {
            this.transaction = transaction;
        }

        @Override
        public IndexDefinition createIndexDefinition(
                Label[] labels,
                String indexName,
                IndexType indexType,
                IndexConfig indexConfig,
                String... propertyKeys) {
            try {
                TokenWrite tokenWrite = transaction.tokenWrite();
                String[] labelNames = Arrays.stream(labels).map(Label::name).toArray(String[]::new);
                int[] labelIds = new int[labels.length];
                tokenWrite.labelGetOrCreateForNames(labelNames, labelIds);
                int[] propertyKeyIds = getOrCreatePropertyKeyIds(tokenWrite, propertyKeys);
                SchemaDescriptor schema;
                if (indexType == IndexType.FULLTEXT) {
                    schema = fulltext(EntityType.NODE, labelIds, propertyKeyIds);
                } else if (labelIds.length == 1) {
                    schema = forLabel(labelIds[0], propertyKeyIds);
                } else {
                    throw new IllegalArgumentException(
                            indexType + " indexes can only be created with exactly one label, " + "but got "
                                    + (labelIds.length == 0 ? "no" : String.valueOf(labelIds.length)) + " labels.");
                }
                IndexDescriptor indexReference = createIndex(indexName, schema, indexType, indexConfig);
                return new IndexDefinitionImpl(this, indexReference, labels, propertyKeys, false);
            } catch (IllegalTokenNameException e) {
                throw new IllegalArgumentException(e);
            } catch (InvalidTransactionTypeKernelException | SchemaKernelException e) {
                throw new ConstraintViolationException(e.getUserMessage(transaction.tokenRead()), e);
            } catch (KernelException e) {
                throw new TransactionFailureException("Unknown error trying to create token ids", e, e.status());
            }
        }

        @Override
        public IndexDefinition createIndexDefinition(
                RelationshipType[] types,
                String indexName,
                IndexType indexType,
                IndexConfig indexConfig,
                String... propertyKeys) {
            try {
                TokenWrite tokenWrite = transaction.tokenWrite();
                String[] typeNames =
                        Arrays.stream(types).map(RelationshipType::name).toArray(String[]::new);
                int[] typeIds = new int[types.length];
                tokenWrite.relationshipTypeGetOrCreateForNames(typeNames, typeIds);
                int[] propertyKeyIds = getOrCreatePropertyKeyIds(tokenWrite, propertyKeys);
                SchemaDescriptor schema;
                if (indexType == IndexType.FULLTEXT) {
                    schema = fulltext(EntityType.RELATIONSHIP, typeIds, propertyKeyIds);
                } else if (typeIds.length == 1) {
                    schema = forRelType(typeIds[0], propertyKeyIds);
                } else {
                    throw new IllegalArgumentException(indexType
                            + " indexes can only be created with exactly one relationship type, " + "but got "
                            + (types.length == 0 ? "no" : String.valueOf(types.length)) + " relationship types.");
                }
                IndexDescriptor indexReference = createIndex(indexName, schema, indexType, indexConfig);
                return new IndexDefinitionImpl(this, indexReference, types, propertyKeys, false);
            } catch (IllegalTokenNameException e) {
                throw new IllegalArgumentException(e);
            } catch (InvalidTransactionTypeKernelException | SchemaKernelException e) {
                throw new ConstraintViolationException(e.getUserMessage(transaction.tokenRead()), e);
            } catch (KernelException e) {
                throw new TransactionFailureException("Unknown error trying to create token ids", e, e.status());
            }
        }

        @Override
        public IndexDefinition createIndexDefinition(AnyTokens tokens, String indexName, IndexConfig indexConfig) {
            try {
                var schema = SchemaDescriptors.forAnyEntityTokens(
                        tokens == AnyTokens.ANY_LABELS ? EntityType.NODE : EntityType.RELATIONSHIP);
                var indexDescriptor = createIndex(indexName, schema, IndexType.LOOKUP, indexConfig);
                if (tokens == AnyTokens.ANY_LABELS) {
                    return new IndexDefinitionImpl(this, indexDescriptor, new Label[0], EMPTY_STRING_ARRAY, false);
                }
                return new IndexDefinitionImpl(
                        this, indexDescriptor, new RelationshipType[0], EMPTY_STRING_ARRAY, false);
            } catch (InvalidTransactionTypeKernelException | SchemaKernelException e) {
                throw new ConstraintViolationException(e.getUserMessage(transaction.tokenRead()), e);
            } catch (KernelException e) {
                throw new TransactionFailureException("Unknown error trying to create index", e, e.status());
            }
        }

        public IndexDescriptor createIndex(
                String indexName, SchemaDescriptor schema, IndexType indexType, IndexConfig indexConfig)
                throws KernelException {
            IndexPrototype prototype = IndexPrototype.forSchema(schema)
                    .withName(indexName)
                    .withIndexType(fromPublicApi(indexType))
                    .withIndexConfig(indexConfig);
            return transaction.schemaWrite().indexCreate(prototype);
        }

        @Override
        public void dropIndexDefinitions(IndexDefinition indexDefinition) {
            try {
                IndexDescriptor reference = getIndexReference(
                        transaction.schemaRead(), transaction.tokenRead(), (IndexDefinitionImpl) indexDefinition);
                transaction.schemaWrite().indexDrop(reference);
            } catch (NotFoundException e) {
                // Silently ignore invalid label and property names
            } catch (SchemaRuleNotFoundException | DropIndexFailureException e) {
                throw new ConstraintViolationException(e.getUserMessage(transaction.tokenRead()), e);
            } catch (InvalidTransactionTypeKernelException | SchemaKernelException e) {
                throw new ConstraintViolationException(e.getMessage(), e);
            }
        }

        @Override
        public ConstraintDefinition createNodePropertyUniquenessConstraint(
                IndexDefinition indexDefinition, String name, IndexType indexType, IndexConfig indexConfig) {
            if (indexDefinition.isMultiTokenIndex()) {
                throw new ConstraintViolationException(
                        "A property uniqueness constraint does not support multi-token index definitions. "
                                + "That is, only a single label is supported, but the following labels were provided: "
                                + labelNameList(indexDefinition.getLabels(), "", "."));
            }
            return createPropertyUniquenessConstraint(
                    indexDefinition,
                    name,
                    indexType,
                    indexConfig,
                    (tokenWrite, indexDef) -> {
                        int labelId = tokenWrite.labelGetOrCreateForName(
                                single(indexDefinition.getLabels()).name());
                        int[] propertyKeyIds = getOrCreatePropertyKeyIds(tokenWrite, indexDefinition);
                        return forLabel(labelId, propertyKeyIds);
                    },
                    NodeUniquenessConstraintDefinition::new);
        }

        @Override
        public ConstraintDefinition createRelationshipPropertyUniquenessConstraint(
                IndexDefinition indexDefinition, String name, IndexType indexType, IndexConfig indexConfig) {
            if (indexDefinition.isMultiTokenIndex()) {
                throw new ConstraintViolationException(
                        "A property uniqueness constraint does not support multi-token index definitions. "
                                + "That is, only a single relationship type is supported, but the following "
                                + "relationship types were provided: "
                                + relTypeNameList(indexDefinition.getRelationshipTypes(), "", "."));
            }
            return createPropertyUniquenessConstraint(
                    indexDefinition,
                    name,
                    indexType,
                    indexConfig,
                    (tokenWrite, indexDef) -> {
                        int typeId = tokenWrite.relationshipTypeGetOrCreateForName(
                                single(indexDefinition.getRelationshipTypes()).name());
                        int[] propertyKeyIds = getOrCreatePropertyKeyIds(tokenWrite, indexDefinition);
                        return forRelType(typeId, propertyKeyIds);
                    },
                    RelationshipUniquenessConstraintDefinition::new);
        }

        @FunctionalInterface
        private interface SchemaDescriptorCreator {
            SchemaDescriptor create(TokenWrite tokenWrite, IndexDefinition indexDefinition) throws KernelException;
        }

        @FunctionalInterface
        private interface ConstraintDefinitionCreator {
            ConstraintDefinition create(
                    InternalSchemaActions actions, ConstraintDescriptor constraint, IndexDefinition indexDefinition)
                    throws KernelException;
        }

        private ConstraintDefinition createPropertyUniquenessConstraint(
                IndexDefinition indexDefinition,
                String name,
                IndexType indexType,
                IndexConfig indexConfig,
                SchemaDescriptorCreator createSchemaDescriptor,
                ConstraintDefinitionCreator createConstraintDefinition) {
            assertConstraintableIndexType("Property uniqueness", indexType);
            return createConstraintWithErrorHandling((transaction) -> {
                TokenWrite tokenWrite = transaction.tokenWrite();
                SchemaDescriptor schema = createSchemaDescriptor.create(tokenWrite, indexDefinition);
                IndexPrototype prototype = IndexPrototype.uniqueForSchema(schema)
                        .withName(name)
                        .withIndexType(fromPublicApi(indexType))
                        .withIndexConfig(indexConfig);
                ConstraintDescriptor constraint = transaction.schemaWrite().uniquePropertyConstraintCreate(prototype);
                return createConstraintDefinition.create(this, constraint, indexDefinition);
            });
        }

        private static void assertConstraintableIndexType(String constraintType, IndexType indexType) {
            if (indexType != null && indexType != IndexType.RANGE) {
                throw new IllegalArgumentException(
                        constraintType + " constraints cannot be created with index type " + indexType + ".");
            }
        }

        @Override
        public ConstraintDefinition createNodeKeyConstraint(
                IndexDefinition indexDefinition, String name, IndexType indexType, IndexConfig indexConfig) {
            if (indexDefinition.isMultiTokenIndex()) {
                throw new ConstraintViolationException(
                        "A node key constraint does not support multi-token index definitions. "
                                + "That is, only a single label is supported, but the following labels were provided: "
                                + labelNameList(indexDefinition.getLabels(), "", "."));
            }
            assertConstraintableIndexType("Node key", indexType);
            return createKeyConstraint(
                    indexDefinition,
                    name,
                    indexType,
                    indexConfig,
                    (tokenWrite, indexDef) -> {
                        int labelId = tokenWrite.labelGetOrCreateForName(
                                single(indexDefinition.getLabels()).name());
                        int[] propertyKeyIds = getOrCreatePropertyKeyIds(tokenWrite, indexDefinition);
                        return forLabel(labelId, propertyKeyIds);
                    },
                    NodeKeyConstraintDefinition::new);
        }

        @Override
        public ConstraintDefinition createRelationshipKeyConstraint(
                IndexDefinition indexDefinition, String name, IndexType indexType, IndexConfig indexConfig) {
            if (indexDefinition.isMultiTokenIndex()) {
                throw new ConstraintViolationException(
                        "A relationship key constraint does not support multi-token index definitions. "
                                + "That is, only a single relationship type is supported, but the "
                                + "following relationship types were provided: "
                                + relTypeNameList(indexDefinition.getRelationshipTypes(), "", "."));
            }
            assertConstraintableIndexType("Relationship key", indexType);
            return createKeyConstraint(
                    indexDefinition,
                    name,
                    indexType,
                    indexConfig,
                    (tokenWrite, indexDef) -> {
                        int typeId = tokenWrite.relationshipTypeGetOrCreateForName(
                                single(indexDef.getRelationshipTypes()).name());
                        int[] propertyKeyIds = getOrCreatePropertyKeyIds(tokenWrite, indexDefinition);
                        return forRelType(typeId, propertyKeyIds);
                    },
                    RelationshipKeyConstraintDefinition::new);
        }

        private ConstraintDefinition createKeyConstraint(
                IndexDefinition indexDefinition,
                String name,
                IndexType indexType,
                IndexConfig indexConfig,
                SchemaDescriptorCreator schemaDescriptorCreator,
                ConstraintDefinitionCreator constraintDefinitionCreator) {
            return createConstraintWithErrorHandling((transaction) -> {
                TokenWrite tokenWrite = transaction.tokenWrite();
                SchemaDescriptor schema = schemaDescriptorCreator.create(tokenWrite, indexDefinition);
                IndexPrototype prototype = IndexPrototype.uniqueForSchema(schema)
                        .withName(name)
                        .withIndexType(fromPublicApi(indexType))
                        .withIndexConfig(indexConfig);
                ConstraintDescriptor constraint = transaction.schemaWrite().keyConstraintCreate(prototype);
                return constraintDefinitionCreator.create(this, constraint, indexDefinition);
            });
        }

        @Override
        public ConstraintDefinition createPropertyExistenceConstraint(
                String name, Label label, String... propertyKeys) {
            return createConstraintWithErrorHandling((transaction) -> {
                TokenWrite tokenWrite = transaction.tokenWrite();
                int labelId = tokenWrite.labelGetOrCreateForName(label.name());
                int[] propertyKeyIds = getOrCreatePropertyKeyIds(tokenWrite, propertyKeys);
                LabelSchemaDescriptor schema = forLabel(labelId, propertyKeyIds);
                ConstraintDescriptor constraint =
                        transaction.schemaWrite().nodePropertyExistenceConstraintCreate(schema, name, false);
                return new NodePropertyExistenceConstraintDefinition(this, constraint, label, propertyKeys);
            });
        }

        @Override
        public ConstraintDefinition createPropertyExistenceConstraint(
                String name, RelationshipType type, String propertyKey) {
            return createConstraintWithErrorHandling((transaction) -> {
                TokenWrite tokenWrite = transaction.tokenWrite();
                int typeId = tokenWrite.relationshipTypeGetOrCreateForName(type.name());
                int[] propertyKeyId = getOrCreatePropertyKeyIds(tokenWrite, propertyKey);
                RelationTypeSchemaDescriptor schema = forRelType(typeId, propertyKeyId);
                ConstraintDescriptor constraint =
                        transaction.schemaWrite().relationshipPropertyExistenceConstraintCreate(schema, name, false);
                return new RelationshipPropertyExistenceConstraintDefinition(this, constraint, type, propertyKey);
            });
        }

        @Override
        public ConstraintDefinition createPropertyTypeConstraint(
                String name, Label label, String propertyKey, PropertyTypeSet allowedTypes) {
            return createConstraintWithErrorHandling((transaction) -> {
                TokenWrite tokenWrite = transaction.tokenWrite();
                int labelId = tokenWrite.labelGetOrCreateForName(label.name());
                int[] propertyKeyId = getOrCreatePropertyKeyIds(tokenWrite, propertyKey);
                LabelSchemaDescriptor schema = forLabel(labelId, propertyKeyId);
                ConstraintDescriptor constraint =
                        transaction.schemaWrite().propertyTypeConstraintCreate(schema, name, allowedTypes, false);
                return new NodePropertyTypeConstraintDefinition(this, constraint, label, propertyKey);
            });
        }

        @Override
        public ConstraintDefinition createPropertyTypeConstraint(
                String name, RelationshipType type, String propertyKey, PropertyTypeSet allowedTypes) {
            return createConstraintWithErrorHandling((transaction) -> {
                TokenWrite tokenWrite = transaction.tokenWrite();
                int typeId = tokenWrite.relationshipTypeGetOrCreateForName(type.name());
                int[] propertyKeyId = getOrCreatePropertyKeyIds(tokenWrite, propertyKey);
                RelationTypeSchemaDescriptor schema = forRelType(typeId, propertyKeyId);
                ConstraintDescriptor constraint =
                        transaction.schemaWrite().propertyTypeConstraintCreate(schema, name, allowedTypes, false);
                return new RelationshipPropertyTypeConstraintDefinition(this, constraint, type, propertyKey);
            });
        }

        @Override
        public void dropConstraint(ConstraintDescriptor constraint) {
            try {
                transaction.schemaWrite().constraintDrop(constraint, false);
            } catch (DropConstraintFailureException e) {
                throw new ConstraintViolationException(e.getUserMessage(transaction.tokenRead()), e);
            } catch (InvalidTransactionTypeKernelException | SchemaKernelException e) {
                throw new ConstraintViolationException(e.getMessage(), e);
            }
        }

        @Override
        public String getUserMessage(KernelException e) {
            return e.getUserMessage(transaction.tokenRead());
        }

        @Override
        public String getUserDescription(IndexDescriptor index) {
            return index == null ? null : index.userDescription(transaction.tokenRead());
        }

        @Override
        public void assertInOpenTransaction() {
            transaction.assertOpen();
        }

        @FunctionalInterface
        private interface Creator {
            ConstraintDefinition createConstraint(KernelTransaction transaction) throws KernelException;
        }

        private ConstraintDefinition createConstraintWithErrorHandling(Creator creator) {
            try {
                return creator.createConstraint(transaction);
            } catch (AlreadyConstrainedException
                    | ConflictingConstraintException
                    | CreateConstraintFailureException
                    | AlreadyIndexedException
                    | RepeatedSchemaComponentException e) {
                throw new ConstraintViolationException(e.getUserMessage(transaction.tokenRead()), e);
            } catch (IllegalTokenNameException e) {
                throw new IllegalArgumentException(e);
            } catch (TokenCapacityExceededKernelException e) {
                throw new IllegalStateException(e);
            } catch (InvalidTransactionTypeKernelException | SchemaKernelException e) {
                throw new ConstraintViolationException(e.getMessage(), e);
            } catch (KernelException e) {
                throw new TransactionFailureException("Unknown error trying to create token ids", e, e.status());
            }
        }
    }
}
