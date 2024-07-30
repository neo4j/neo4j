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
package org.neo4j.storageengine.api.txstate;

import java.util.function.Function;
import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.Upgrade;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.storageengine.api.StorageProperty;

/**
 * A visitor for visiting the changes that have been made in a transaction.
 */
public interface TxStateVisitor extends AutoCloseable {
    void visitCreatedNode(long id);

    void visitDeletedNode(long id);

    void visitRelationshipModifications(RelationshipModifications modifications) throws ConstraintValidationException;

    void visitNodePropertyChanges(
            long id, Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed)
            throws ConstraintValidationException;

    /**
     * Only called for property changes on existing relationships. For created relationships the relationship properties will be trickle in
     * via {@link #visitRelationshipModifications(RelationshipModifications)}.
     */
    void visitRelPropertyChanges(
            long id,
            int type,
            long startNode,
            long endNode,
            Iterable<StorageProperty> added,
            Iterable<StorageProperty> changed,
            IntIterable removed)
            throws ConstraintValidationException;

    void visitNodeLabelChanges(long id, LongSet added, LongSet removed) throws ConstraintValidationException;

    void visitAddedIndex(IndexDescriptor element) throws KernelException;

    void visitRemovedIndex(IndexDescriptor element);

    void visitAddedConstraint(ConstraintDescriptor element) throws KernelException;

    void visitRemovedConstraint(ConstraintDescriptor element);

    void visitCreatedLabelToken(long id, String name, boolean internal);

    void visitCreatedPropertyKeyToken(long id, String name, boolean internal);

    void visitCreatedRelationshipTypeToken(long id, String name, boolean internal);

    void visitKernelUpgrade(Upgrade.KernelUpgrade kernelUpgrade);

    @Override
    void close() throws KernelException;

    class Adapter implements TxStateVisitor {
        @Override
        public void visitCreatedNode(long id) {}

        @Override
        public void visitDeletedNode(long id) {}

        @Override
        public void visitRelationshipModifications(RelationshipModifications modifications)
                throws ConstraintValidationException {}

        @Override
        public void visitNodePropertyChanges(
                long id, Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed)
                throws ConstraintValidationException {}

        @Override
        public void visitRelPropertyChanges(
                long id,
                int type,
                long startNode,
                long endNode,
                Iterable<StorageProperty> added,
                Iterable<StorageProperty> changed,
                IntIterable removed)
                throws ConstraintValidationException {}

        @Override
        public void visitNodeLabelChanges(long id, LongSet added, LongSet removed)
                throws ConstraintValidationException {}

        @Override
        public void visitAddedIndex(IndexDescriptor index) throws KernelException {}

        @Override
        public void visitRemovedIndex(IndexDescriptor index) {}

        @Override
        public void visitAddedConstraint(ConstraintDescriptor element) throws KernelException {}

        @Override
        public void visitRemovedConstraint(ConstraintDescriptor element) {}

        @Override
        public void visitCreatedLabelToken(long id, String name, boolean internal) {}

        @Override
        public void visitCreatedPropertyKeyToken(long id, String name, boolean internal) {}

        @Override
        public void visitCreatedRelationshipTypeToken(long id, String name, boolean internal) {}

        @Override
        public void visitKernelUpgrade(Upgrade.KernelUpgrade kernelUpgrade) {}

        @Override
        public void close() {}
    }

    TxStateVisitor EMPTY = new Adapter();

    class Delegator implements TxStateVisitor {
        private final TxStateVisitor actual;

        public Delegator(TxStateVisitor actual) {
            assert actual != null;
            this.actual = actual;
        }

        @Override
        public void visitCreatedNode(long id) {
            actual.visitCreatedNode(id);
        }

        @Override
        public void visitDeletedNode(long id) {
            actual.visitDeletedNode(id);
        }

        @Override
        public void visitRelationshipModifications(RelationshipModifications modifications)
                throws ConstraintValidationException {
            actual.visitRelationshipModifications(modifications);
        }

        @Override
        public void visitNodePropertyChanges(
                long id, Iterable<StorageProperty> added, Iterable<StorageProperty> changed, IntIterable removed)
                throws ConstraintValidationException {
            actual.visitNodePropertyChanges(id, added, changed, removed);
        }

        @Override
        public void visitRelPropertyChanges(
                long id,
                int type,
                long startNode,
                long endNode,
                Iterable<StorageProperty> added,
                Iterable<StorageProperty> changed,
                IntIterable removed)
                throws ConstraintValidationException {
            actual.visitRelPropertyChanges(id, type, startNode, endNode, added, changed, removed);
        }

        @Override
        public void visitNodeLabelChanges(long id, LongSet added, LongSet removed)
                throws ConstraintValidationException {
            actual.visitNodeLabelChanges(id, added, removed);
        }

        @Override
        public void visitAddedIndex(IndexDescriptor index) throws KernelException {
            actual.visitAddedIndex(index);
        }

        @Override
        public void visitRemovedIndex(IndexDescriptor index) {
            actual.visitRemovedIndex(index);
        }

        @Override
        public void visitAddedConstraint(ConstraintDescriptor constraint) throws KernelException {
            actual.visitAddedConstraint(constraint);
        }

        @Override
        public void visitRemovedConstraint(ConstraintDescriptor constraint) {
            actual.visitRemovedConstraint(constraint);
        }

        @Override
        public void visitCreatedLabelToken(long id, String name, boolean internal) {
            actual.visitCreatedLabelToken(id, name, internal);
        }

        @Override
        public void visitCreatedPropertyKeyToken(long id, String name, boolean internal) {
            actual.visitCreatedPropertyKeyToken(id, name, internal);
        }

        @Override
        public void visitCreatedRelationshipTypeToken(long id, String name, boolean internal) {
            actual.visitCreatedRelationshipTypeToken(id, name, internal);
        }

        @Override
        public void visitKernelUpgrade(Upgrade.KernelUpgrade kernelUpgrade) {
            actual.visitKernelUpgrade(kernelUpgrade);
        }

        @Override
        public void close() throws KernelException {
            actual.close();
        }
    }

    /**
     * Interface for allowing decoration of a TxStateVisitor with one or more other visitor(s).
     */
    interface Decorator extends Function<TxStateVisitor, TxStateVisitor> {}

    Decorator NO_DECORATION = txStateVisitor -> txStateVisitor;
}
