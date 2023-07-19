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
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenNotFoundException;

public class ReadOnlyTokenRead implements TokenRead {
    private final TokenHolders tokenHolders;

    public ReadOnlyTokenRead(TokenHolders tokenHolders) {
        this.tokenHolders = tokenHolders;
    }

    @Override
    public int nodeLabel(String name) {
        return tokenHolders.labelTokens().getIdByName(name);
    }

    @Override
    public String nodeLabelName(int labelId) throws LabelNotFoundKernelException {
        try {
            return tokenHolders.labelTokens().getTokenById(labelId).name();
        } catch (TokenNotFoundException e) {
            throw new LabelNotFoundKernelException(labelId, e);
        }
    }

    @Override
    public int relationshipType(String name) {
        return tokenHolders.relationshipTypeTokens().getIdByName(name);
    }

    @Override
    public String relationshipTypeName(int relationshipTypeId) throws RelationshipTypeIdNotFoundKernelException {
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
        return tokenHolders.propertyKeyTokens().getIdByName(name);
    }

    @Override
    public String propertyKeyName(int propertyKeyId) throws PropertyKeyIdNotFoundKernelException {
        try {
            return tokenHolders.propertyKeyTokens().getTokenById(propertyKeyId).name();
        } catch (TokenNotFoundException e) {
            throw new PropertyKeyIdNotFoundKernelException(propertyKeyId, e);
        }
    }

    @Override
    public Iterator<NamedToken> labelsGetAllTokens() {
        return tokenHolders.labelTokens().getAllTokens().iterator();
    }

    @Override
    public Iterator<NamedToken> propertyKeyGetAllTokens() {
        return tokenHolders.propertyKeyTokens().getAllTokens().iterator();
    }

    @Override
    public Iterator<NamedToken> relationshipTypesGetAllTokens() {
        return tokenHolders.relationshipTypeTokens().getAllTokens().iterator();
    }

    @Override
    public int labelCount() {
        return tokenHolders.labelTokens().size();
    }

    @Override
    public int propertyKeyCount() {
        return tokenHolders.propertyKeyTokens().size();
    }

    @Override
    public int relationshipTypeCount() {
        return tokenHolders.relationshipTypeTokens().size();
    }

    @Override
    public String labelGetName(int labelId) {
        return tokenHolders.labelGetName(labelId);
    }

    @Override
    public String relationshipTypeGetName(int relationshipTypeId) {
        return tokenHolders.relationshipTypeGetName(relationshipTypeId);
    }

    @Override
    public String propertyKeyGetName(int propertyKeyId) {
        return tokenHolders.propertyKeyGetName(propertyKeyId);
    }
}
