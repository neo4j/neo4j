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
package org.neo4j.kernel.api.impl.schema;

import java.io.IOException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.schema.reader.IndexReaderCloseException;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexProgressor.EntityValueClient;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracking;

public abstract class AbstractTextIndexReader extends AbstractLuceneIndexReader {
    private final SearcherReference searcherReference;

    protected AbstractTextIndexReader(
            IndexDescriptor descriptor, SearcherReference searcherReference, IndexUsageTracking usageTracker) {
        super(descriptor, usageTracker);
        this.searcherReference = searcherReference;
    }

    @Override
    protected IndexProgressor indexProgressor(
            Query query, IndexQueryConstraints constraints, EntityValueClient client) {
        return search(getIndexSearcher(), query).getIndexProgressor(entityIdFieldKey(), client);
    }

    @Override
    public void close() {
        try (searcherReference) {
            super.close();
        } catch (IOException e) {
            throw new IndexReaderCloseException(e);
        }
    }

    protected IndexSearcher getIndexSearcher() {
        return searcherReference.getIndexSearcher();
    }
}
