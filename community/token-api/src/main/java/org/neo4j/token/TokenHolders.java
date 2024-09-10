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
package org.neo4j.token;

import static org.neo4j.token.ReadOnlyTokenCreator.READ_ONLY;

import java.util.List;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;
import org.neo4j.token.api.TokensLoader;

/**
 * Holds onto all available {@link TokenHolder} for easily passing all those around
 * and for easily extending available instances in one place.
 * Resolves token names without going through transaction and locking layers.
 */
public record TokenHolders(TokenHolder propertyKeyTokens, TokenHolder labelTokens, TokenHolder relationshipTypeTokens)
        implements TokenNameLookup {

    public void setInitialTokens(TokensLoader loader, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        propertyKeyTokens().setInitialTokens(loader.getPropertyKeyTokens(storeCursors, memoryTracker));
        labelTokens().setInitialTokens(loader.getLabelTokens(storeCursors, memoryTracker));
        relationshipTypeTokens().setInitialTokens(loader.getRelationshipTypeTokens(storeCursors, memoryTracker));
    }

    @Override
    public String labelGetName(int labelId) {
        try {
            return labelTokens().getTokenById(labelId).name();
        } catch (TokenNotFoundException e) {
            return "[no such label: " + labelId + "]";
        }
    }

    @Override
    public String relationshipTypeGetName(int relationshipTypeId) {
        try {
            return relationshipTypeTokens().getTokenById(relationshipTypeId).name();
        } catch (TokenNotFoundException e) {
            return "[no such relationship type: " + relationshipTypeId + "]";
        }
    }

    @Override
    public String propertyKeyGetName(int propertyKeyId) {
        try {
            return propertyKeyTokens().getTokenById(propertyKeyId).name();
        } catch (TokenNotFoundException e) {
            return "[no such property key: " + propertyKeyId + "]";
        }
    }

    /**
     * Produce a {@link TokenNameLookup} that appends the token ids to all the token names.
     */
    public TokenNameLookup lookupWithIds() {
        return new TokenNameLookup() {
            @Override
            public String labelGetName(int labelId) {
                return TokenHolders.this.labelGetName(labelId) + "[" + labelId + "]";
            }

            @Override
            public String relationshipTypeGetName(int relationshipTypeId) {
                return TokenHolders.this.relationshipTypeGetName(relationshipTypeId) + "[" + relationshipTypeId + "]";
            }

            @Override
            public String propertyKeyGetName(int propertyKeyId) {
                return TokenHolders.this.propertyKeyGetName(propertyKeyId) + "[" + propertyKeyId + "]";
            }
        };
    }

    /**
     * @param label the label whose ID is required
     * @return the ID that represents the provided label
     */
    public int labelForName(String label) {
        return labelTokens().getIdByName(label);
    }

    /**
     * @param labels the labels whose IDs are required
     * @return the IDs that represents the provided labels
     */
    public int[] labelsForNames(List<String> labels) {
        return idsForNames(labelTokens(), labels);
    }

    /**
     * @param relType the relationship whose ID is required
     * @return the ID that represents the provided relationship
     */
    public int relationshipForName(String relType) {
        return relationshipTypeTokens().getIdByName(relType);
    }

    /**
     * @param relTypes the relationships whose IDs are required
     * @return the IDs that represents the provided relationships
     */
    public int[] relationshipsForNames(List<String> relTypes) {
        return idsForNames(relationshipTypeTokens(), relTypes);
    }

    /**
     * @param property the property whose ID is required
     * @return the ID that represents the provided property
     */
    public int propertyForName(String property) {
        return propertyKeyTokens().getIdByName(property);
    }

    /**
     * @param properties the properties whose IDs are required
     * @return the IDs that represents the provided properties
     */
    public int[] propertiesForName(List<String> properties) {
        return idsForNames(propertyKeyTokens(), properties);
    }

    /**
     * @param loader the token loader
     * @param storeCursors cursors used to load the tokens
     * @return all the tokens
     */
    public static TokenHolders readOnlyTokenHolders(
            TokensLoader loader, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        var tokenHolders = new TokenHolders(
                new CreatingTokenHolder(READ_ONLY, TokenHolder.TYPE_PROPERTY_KEY),
                new CreatingTokenHolder(READ_ONLY, TokenHolder.TYPE_LABEL),
                new CreatingTokenHolder(READ_ONLY, TokenHolder.TYPE_RELATIONSHIP_TYPE));
        tokenHolders.setInitialTokens(loader, storeCursors, memoryTracker);
        return tokenHolders;
    }

    private static int[] idsForNames(TokenHolder holder, List<String> tokens) {
        final var ids = new int[tokens.size()];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = holder.getIdByName(tokens.get(i));
        }
        return ids;
    }
}
