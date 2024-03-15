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

import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;

class BridgingCacheAccess implements CacheAccessBackDoor {
    private final SchemaCache schemaCache;
    private final SchemaState schemaState;
    private final TokenHolders tokenHolders;

    BridgingCacheAccess(SchemaCache schemaCache, SchemaState schemaState, TokenHolders tokenHolders) {
        this.schemaCache = schemaCache;
        this.schemaState = schemaState;
        this.tokenHolders = tokenHolders;
    }

    @Override
    public void addSchemaRule(SchemaRule rule) {
        schemaCache.addSchemaRule(rule);
    }

    @Override
    public void removeSchemaRuleFromCache(long id) {
        schemaCache.removeSchemaRule(id);
        schemaState.clear();
    }

    @Override
    public void addRelationshipTypeToken(NamedToken type, boolean atomic) {
        tokenHolders.relationshipTypeTokens().addToken(type, atomic);
    }

    @Override
    public void addLabelToken(NamedToken label, boolean atomic) {
        tokenHolders.labelTokens().addToken(label, atomic);
    }

    @Override
    public void addPropertyKeyToken(NamedToken propertyKey, boolean atomic) {
        tokenHolders.propertyKeyTokens().addToken(propertyKey, atomic);
    }
}
