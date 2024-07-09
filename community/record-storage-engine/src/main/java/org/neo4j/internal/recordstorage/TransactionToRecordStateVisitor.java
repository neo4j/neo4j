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
package org.neo4j.internal.recordstorage;

import java.util.OptionalLong;
import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.Upgrade;
import org.neo4j.internal.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.internal.schema.constraints.KeyConstraintDescriptor;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.RelationshipModifications;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.storageengine.api.txstate.validation.TransactionConflictException;

class TransactionToRecordStateVisitor extends TxStateVisitor.Adapter {
    private boolean clearSchemaState;
    private final TransactionRecordState recordState;
    private final SchemaState schemaState;
    private final SchemaRuleAccess schemaStorage;
    private final SchemaRecordChangeTranslator schemaStateChanger;
    private final ConstraintRuleAccessor constraintSemantics;
    private final CursorContext cursorContext;
    private final StoreCursors storeCursors;
    private final boolean transientMissingSchema;

    TransactionToRecordStateVisitor(
            TransactionRecordState recordState,
            SchemaState schemaState,
            SchemaRuleAccess schemaRuleAccess,
            ConstraintRuleAccessor constraintSemantics,
            CursorContext cursorContext,
            StoreCursors storeCursors,
            boolean transientMissingSchema) {
        this.recordState = recordState;
        this.schemaState = schemaState;
        this.schemaStorage = schemaRuleAccess;
        this.schemaStateChanger = schemaRuleAccess.getSchemaRecordChangeTranslator();
        this.constraintSemantics = constraintSemantics;
        this.cursorContext = cursorContext;
        this.storeCursors = storeCursors;
        this.transientMissingSchema = transientMissingSchema;
    }

    @Override
    public void close() {
        try {
            if (clearSchemaState) {
                schemaState.clear();
            }
        } finally {
            clearSchemaState = false;
        }
    }

    @Override
    public void visitCreatedNode(long id) {
        recordState.nodeCreate(id);
    }

    @Override
    public void visitDeletedNode(long id) {
        recordState.nodeDelete(id);
    }

    @Override
    public void visitRelationshipModifications(RelationshipModifications modifications) {
        recordState.relModify(modifications);
        modifications.creations().forEach((id, t, s, e, properties) -> visitAddedRelProperties(id, properties));
    }

    @Override
    public void visitNodePropertyChanges(
            long id, Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed) {
        removed.each(propId -> recordState.nodeRemoveProperty(id, propId));
        for (StorageProperty property : changed) {
            recordState.nodeChangeProperty(id, property.propertyKeyId(), property.value());
        }
        for (StorageProperty property : added) {
            recordState.nodeAddProperty(id, property.propertyKeyId(), property.value());
        }
    }

    @Override
    public void visitRelPropertyChanges(
            long id,
            int type,
            long startNode,
            long endNode,
            Iterable<StorageProperty> added,
            Iterable<StorageProperty> changed,
            IntIterable removed) {
        removed.each(relId -> recordState.relRemoveProperty(id, relId));
        for (StorageProperty property : changed) {
            recordState.relChangeProperty(id, property.propertyKeyId(), property.value());
        }
        visitAddedRelProperties(id, added);
    }

    private void visitAddedRelProperties(long id, Iterable<StorageProperty> added) {
        for (StorageProperty property : added) {
            recordState.relAddProperty(id, property.propertyKeyId(), property.value());
        }
    }

    @Override
    public void visitNodeLabelChanges(long id, final LongSet added, final LongSet removed) {
        // record the state changes to be made to the store
        removed.each(label -> recordState.removeLabelFromNode((int) label, id));
        added.each(label -> recordState.addLabelToNode((int) label, id));
    }

    @Override
    public void visitAddedIndex(IndexDescriptor index) throws KernelException {
        schemaStateChanger.createSchemaRule(recordState, index);
    }

    @Override
    public void visitRemovedIndex(IndexDescriptor index) {
        schemaStateChanger.dropSchemaRule(recordState, index);
    }

    @Override
    public void visitAddedConstraint(ConstraintDescriptor constraint) throws KernelException {
        clearSchemaState = true;
        long constraintId = schemaStorage.newRuleId(cursorContext);

        switch (constraint.type()) {
            case UNIQUE -> visitAddedUniquenessConstraint(constraint.asUniquenessConstraint(), constraintId);
            case UNIQUE_EXISTS -> visitAddedKeyConstraint(constraint.asKeyConstraint(), constraintId);
            case EXISTS -> {
                ConstraintDescriptor rule = constraintSemantics.createExistenceConstraint(constraintId, constraint);
                schemaStateChanger.createSchemaRule(recordState, rule);
            }
            case PROPERTY_TYPE -> {
                ConstraintDescriptor rule = constraintSemantics.createPropertyTypeConstraint(
                        constraintId, constraint.asPropertyTypeConstraint());
                schemaStateChanger.createSchemaRule(recordState, rule);
            }
            case ENDPOINT -> {
                ConstraintDescriptor rule = constraintSemantics.createRelationshipEndpointConstraint(
                        constraintId, constraint.asRelationshipEndpointConstraint());
                schemaStateChanger.createSchemaRule(recordState, rule);
            }
            default -> throw new IllegalStateException(constraint.type().toString());
        }
    }

    @Override
    public void visitKernelUpgrade(Upgrade.KernelUpgrade kernelUpgrade) {
        recordState.upgrade(kernelUpgrade);
    }

    private void visitAddedUniquenessConstraint(UniquenessConstraintDescriptor uniqueConstraint, long constraintId)
            throws KernelException {
        IndexDescriptor indexRule =
                (IndexDescriptor) schemaStorage.loadSingleSchemaRule(uniqueConstraint.ownedIndexId(), storeCursors);
        ConstraintDescriptor constraint =
                constraintSemantics.createUniquenessConstraintRule(constraintId, uniqueConstraint, indexRule.getId());
        schemaStateChanger.createSchemaRule(recordState, constraint);
        schemaStateChanger.setConstraintIndexOwner(recordState, indexRule, constraintId);
    }

    private void visitAddedKeyConstraint(KeyConstraintDescriptor uniqueConstraint, long constraintId)
            throws KernelException {
        IndexDescriptor indexRule =
                (IndexDescriptor) schemaStorage.loadSingleSchemaRule(uniqueConstraint.ownedIndexId(), storeCursors);
        ConstraintDescriptor constraint =
                constraintSemantics.createKeyConstraintRule(constraintId, uniqueConstraint, indexRule.getId());
        schemaStateChanger.createSchemaRule(recordState, constraint);
        schemaStateChanger.setConstraintIndexOwner(recordState, indexRule, constraintId);
    }

    @Override
    public void visitRemovedConstraint(ConstraintDescriptor constraint) {
        clearSchemaState = true;
        try {
            ConstraintDescriptor rule = schemaStorage.constraintsGetSingle(constraint, storeCursors);
            schemaStateChanger.dropSchemaRule(recordState, rule);

            if (constraint.enforcesUniqueness()) {
                // Remove the index for the constraint as well
                IndexDescriptor[] indexes = schemaStorage.indexGetForSchema(constraint, storeCursors);
                for (IndexDescriptor index : indexes) {
                    OptionalLong owningConstraintId = index.getOwningConstraintId();
                    if (owningConstraintId.isPresent() && owningConstraintId.getAsLong() == rule.getId()) {
                        visitRemovedIndex(index);
                    }
                    // Note that we _could_ also go through all the matching indexes that have isUnique == true and no
                    // owning constraint id, and remove those
                    // as well. These might be orphaned indexes from failed constraint creations. However, since we want
                    // to allow multiple indexes and
                    // constraints on the same schema, they could also be constraint indexes that are currently
                    // populating for other constraints, and if that's
                    // the case, then we cannot remove them, since that would ruin the constraint they are being built
                    // for.
                }
            }
        } catch (SchemaRuleNotFoundException e) {
            if (transientMissingSchema) {
                throw new TransactionConflictException(
                        "Concurrent modification exception. Constraint to be removed already removed by another transaction.",
                        e);
            }
            throw new IllegalStateException(
                    "Constraint to be removed should exist, since its existence should have been validated earlier "
                            + "and the schema should have been locked.",
                    e);
        } catch (DuplicateSchemaRuleException e) {
            throw new IllegalStateException("Multiple constraints found for specified label and property.", e);
        }
    }

    @Override
    public void visitCreatedLabelToken(long id, String name, boolean internal) {
        recordState.createLabelToken(name, id, internal);
    }

    @Override
    public void visitCreatedPropertyKeyToken(long id, String name, boolean internal) {
        recordState.createPropertyKeyToken(name, id, internal);
    }

    @Override
    public void visitCreatedRelationshipTypeToken(long id, String name, boolean internal) {
        recordState.createRelationshipTypeToken(name, id, internal);
    }
}
