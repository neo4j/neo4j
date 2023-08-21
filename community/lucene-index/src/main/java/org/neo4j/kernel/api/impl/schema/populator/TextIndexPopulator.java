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
package org.neo4j.kernel.api.impl.schema.populator;

import org.apache.lucene.document.Document;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.schema.TextDocumentStructure;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;

/**
 * A {@link LuceneIndexPopulator} used for non-unique Lucene schema indexes, Performs index sampling.
 */
public class TextIndexPopulator extends LuceneIndexPopulator<DatabaseIndex<ValueIndexReader>> {

    public TextIndexPopulator(DatabaseIndex<ValueIndexReader> luceneIndex, IndexUpdateIgnoreStrategy ignoreStrategy) {
        super(luceneIndex, ignoreStrategy);
    }

    @Override
    protected Document updateAsDocument(ValueIndexEntryUpdate<?> update) {
        return TextDocumentStructure.documentRepresentingProperties(update.getEntityId(), update.values());
    }

    @Override
    public IndexUpdater newPopulatingUpdater(CursorContext cursorContext) {
        return new TextIndexPopulatingUpdater(writer, ignoreStrategy);
    }
}
