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
package org.neo4j.kernel.impl.newapi;

import java.util.Iterator;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.impl.api.OverridableSecurityContext;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenNotFoundException;

/**
 * See {@link KernelRead} for the description of the concurrency semantic and what resources
 * are allowed to be used by this class. These two classes are very closely related in their use
 * and scoping and everything described in {@link KernelRead} javadoc applies here, too.
 * <p>
 * This class has two implementations on Kernel level.
 * {@link KernelToken} has one-to-one relation with a transaction and therefore it can safely use
 * any transaction-scoped resource.
 * {@link ForThreadExecutionContextScope} has one-to-many relation with a transaction and therefore it CANNOT safely
 * use transaction-scoped resource which is not designed for concurrent use.
 */
public abstract class KernelTokenRead implements TokenRead {

    private final StorageReader store;
    private final TokenHolders tokenHolders;

    KernelTokenRead(StorageReader store, TokenHolders tokenHolders) {
        this.store = store;
        this.tokenHolders = tokenHolders;
    }

    @Override
    public String labelGetName(int labelId) {
        performCheckBeforeOperation();
        return tokenHolders.labelGetName(labelId);
    }

    @Override
    public String relationshipTypeGetName(int relationshipTypeId) {
        performCheckBeforeOperation();
        return tokenHolders.relationshipTypeGetName(relationshipTypeId);
    }

    @Override
    public String propertyKeyGetName(int propertyKeyId) {
        performCheckBeforeOperation();
        return tokenHolders.propertyKeyGetName(propertyKeyId);
    }

    @Override
    public int nodeLabel(String name) {
        performCheckBeforeOperation();
        return tokenHolders.labelTokens().getIdByName(name);
    }

    @Override
    public String nodeLabelName(int labelId) throws LabelNotFoundKernelException {
        performCheckBeforeOperation();
        try {
            return tokenHolders.labelTokens().getTokenById(labelId).name();
        } catch (TokenNotFoundException e) {
            throw new LabelNotFoundKernelException(labelId, e);
        }
    }

    @Override
    public int relationshipType(String name) {
        performCheckBeforeOperation();
        return tokenHolders.relationshipTypeTokens().getIdByName(name);
    }

    @Override
    public String relationshipTypeName(int relationshipTypeId) throws RelationshipTypeIdNotFoundKernelException {
        performCheckBeforeOperation();
        try {
            return tokenHolders
                    .relationshipTypeTokens()
                    .getTokenById(relationshipTypeId)
                    .name();
        } catch (TokenNotFoundException e) {
            throw new RelationshipTypeIdNotFoundKernelException(relationshipTypeId, e);
        }
    }

    @Override
    public int propertyKey(String name) {
        performCheckBeforeOperation();
        return tokenHolders.propertyKeyTokens().getIdByName(name);
    }

    @Override
    public String propertyKeyName(int propertyKeyId) throws PropertyKeyIdNotFoundKernelException {
        performCheckBeforeOperation();
        try {
            return tokenHolders.propertyKeyTokens().getTokenById(propertyKeyId).name();
        } catch (TokenNotFoundException e) {
            throw new PropertyKeyIdNotFoundKernelException(propertyKeyId, e);
        }
    }

    @Override
    public Iterator<NamedToken> labelsGetAllTokens() {
        performCheckBeforeOperation();
        return Iterators.stream(tokenHolders.labelTokens().getAllTokens().iterator())
                .filter(label -> getAccessMode().allowsTraverseNode(label.id())
                        || getAccessMode().hasApplicableTraverseAllowPropertyRules(label.id()))
                .iterator();
    }

    @Override
    public Iterator<NamedToken> propertyKeyGetAllTokens() {
        performCheckBeforeOperation();
        return Iterators.stream(tokenHolders.propertyKeyTokens().getAllTokens().iterator())
                .filter(propKey -> getAccessMode().allowsSeePropertyKeyToken(propKey.id()))
                .iterator();
    }

    @Override
    public Iterator<NamedToken> relationshipTypesGetAllTokens() {
        performCheckBeforeOperation();
        return Iterators.stream(
                        tokenHolders.relationshipTypeTokens().getAllTokens().iterator())
                .filter(relType -> getAccessMode().allowsTraverseRelType(relType.id()))
                .iterator();
    }

    @Override
    public int labelCount() {
        performCheckBeforeOperation();
        return store.labelCount();
    }

    @Override
    public int propertyKeyCount() {
        performCheckBeforeOperation();
        return store.propertyKeyCount();
    }

    @Override
    public int relationshipTypeCount() {
        performCheckBeforeOperation();
        return store.relationshipTypeCount();
    }

    abstract void performCheckBeforeOperation();

    abstract AccessMode getAccessMode();

    public static class ForThreadExecutionContextScope extends KernelTokenRead {

        private final OverridableSecurityContext overridableSecurityContext;
        private final AssertOpen assertOpen;

        public ForThreadExecutionContextScope(
                StorageReader store,
                TokenHolders tokenHolders,
                OverridableSecurityContext overridableSecurityContext,
                AssertOpen assertOpen) {
            super(store, tokenHolders);

            this.overridableSecurityContext = overridableSecurityContext;
            this.assertOpen = assertOpen;
        }

        @Override
        void performCheckBeforeOperation() {
            assertOpen.assertOpen();
        }

        @Override
        AccessMode getAccessMode() {
            return overridableSecurityContext.currentSecurityContext().mode();
        }
    }
}
