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

import static org.neo4j.kernel.impl.index.schema.IndexUsageTracking.NO_USAGE_TRACKING;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndexAccessor;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.IndexValueValidator;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.values.storable.Value;

public class TrigramIndexAccessor
        extends AbstractLuceneIndexAccessor<ValueIndexReader, DatabaseIndex<ValueIndexReader>> {
    private final IndexValueValidator validator;

    public TrigramIndexAccessor(
            DatabaseIndex<ValueIndexReader> luceneIndex,
            IndexDescriptor descriptor,
            IndexUpdateIgnoreStrategy ignoreStrategy,
            IndexValueValidator validator) {
        super(luceneIndex, descriptor, ignoreStrategy);
        this.validator = validator;
    }

    @Override
    protected IndexUpdater getIndexUpdater(IndexUpdateMode mode) {
        return new Updater(mode.requiresIdempotency(), mode.requiresRefresh());
    }

    @Override
    public BoundedIterable<Long> newAllEntriesValueReader(
            long fromIdInclusive, long toIdExclusive, CursorContext cursorContext) {
        try {
            return ((TrigramIndexReader) luceneIndex.getIndexReader(NO_USAGE_TRACKING))
                    .newAllEntriesValueReader(fromIdInclusive, toIdExclusive);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void validateBeforeCommit(long entityId, Value[] tuple) {
        validator.validate(entityId, tuple);
    }

    private class Updater extends AbstractLuceneIndexUpdater {

        Updater(boolean idempotent, boolean refresh) {
            super(idempotent, refresh);
        }

        @Override
        protected void addIdempotent(long entityId, Value[] values) {
            try {
                Document document = TrigramDocumentStructure.createLuceneDocument(entityId, values[0]);
                writer.updateOrDeleteDocument(TrigramDocumentStructure.newTermForChangeOrRemove(entityId), document);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        protected void add(long entityId, Value[] values) {
            try {
                Document document = TrigramDocumentStructure.createLuceneDocument(entityId, values[0]);
                writer.nullableAddDocument(document);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        protected void change(long entityId, Value[] values) {
            addIdempotent(entityId, values);
        }

        @Override
        protected void remove(long entityId) {
            try {
                Term term = TrigramDocumentStructure.newTermForChangeOrRemove(entityId);
                writer.deleteDocuments(term);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
