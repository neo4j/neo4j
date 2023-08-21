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
package org.neo4j.kernel.api.impl.schema.trigram;

import org.apache.lucene.document.Document;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.schema.populator.LuceneIndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.IndexValueValidator;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;

class TrigramIndexPopulator extends LuceneIndexPopulator<DatabaseIndex<ValueIndexReader>> {
    private final IndexValueValidator validator;

    TrigramIndexPopulator(
            DatabaseIndex<ValueIndexReader> luceneIndex,
            IndexUpdateIgnoreStrategy ignoreStrategy,
            IndexValueValidator validator) {
        super(luceneIndex, ignoreStrategy);
        this.validator = validator;
    }

    @Override
    protected Document updateAsDocument(ValueIndexEntryUpdate<?> update) {
        var entityId = update.getEntityId();
        var value = update.values()[0];
        validator.validate(entityId, value);
        return TrigramDocumentStructure.createLuceneDocument(entityId, value);
    }

    @Override
    public IndexUpdater newPopulatingUpdater(CursorContext cursorContext) {
        return new TrigramIndexPopulatingUpdater(writer, ignoreStrategy, validator);
    }
}
