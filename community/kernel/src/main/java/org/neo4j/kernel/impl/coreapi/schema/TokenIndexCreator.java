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

import static org.neo4j.graphdb.schema.IndexSettingUtil.toIndexConfigFromIndexSettingObjectMap;

import java.util.Map;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.schema.AnyTokens;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.schema.IndexConfig;

public class TokenIndexCreator implements IndexCreator {
    private final InternalSchemaActions actions;
    private final AnyTokens tokens;
    private final String indexName;
    private final IndexConfig indexConfig;

    public TokenIndexCreator(InternalSchemaActions actions, AnyTokens tokens) {
        this(actions, tokens, null, null);
    }

    public TokenIndexCreator(
            InternalSchemaActions actions, AnyTokens tokens, String indexName, IndexConfig indexConfig) {
        this.actions = actions;
        this.tokens = tokens;
        this.indexName = indexName;
        this.indexConfig = indexConfig;

        actions.assertInOpenTransaction();
    }

    @Override
    public IndexCreator on(String propertyKey) {
        actions.assertInOpenTransaction();
        throw new ConstraintViolationException("LOOKUP indexes doesn't support inclusion of property keys.");
    }

    @Override
    public IndexCreator withName(String indexName) {
        actions.assertInOpenTransaction();
        return new TokenIndexCreator(actions, tokens, indexName, indexConfig);
    }

    @Override
    public IndexCreator withIndexType(IndexType type) {
        actions.assertInOpenTransaction();
        if (type != IndexType.LOOKUP) {
            throw new ConstraintViolationException("Only LOOKUP index type supported for token indexes.");
        }
        return this;
    }

    @Override
    public IndexCreator withIndexConfiguration(Map<IndexSetting, Object> indexConfiguration) {
        actions.assertInOpenTransaction();
        return new TokenIndexCreator(
                actions, tokens, indexName, toIndexConfigFromIndexSettingObjectMap(indexConfiguration));
    }

    @Override
    public IndexDefinition create() throws ConstraintViolationException {
        actions.assertInOpenTransaction();
        return actions.createIndexDefinition(tokens, indexName, indexConfig);
    }
}
