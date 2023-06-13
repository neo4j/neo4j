package org.neo4j.kernel.api.impl.schema.vector;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndexAccessor;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.Value;

class VectorIndexAccessor extends AbstractLuceneIndexAccessor<ValueIndexReader, DatabaseIndex<ValueIndexReader>> {
    private final VectorSimilarityFunction similarityFunction;

    protected VectorIndexAccessor(
            DatabaseIndex<ValueIndexReader> luceneIndex,
            IndexDescriptor descriptor,
            IndexUpdateIgnoreStrategy ignoreStrategy,
            VectorSimilarityFunction similarityFunction) {
        super(luceneIndex, descriptor, ignoreStrategy);
        this.similarityFunction = similarityFunction;
    }

    @Override
    public BoundedIterable<Long> newAllEntriesValueReader(
            long fromIdInclusive, long toIdExclusive, CursorContext cursorContext) {
        return super.newAllEntriesReader(VectorDocumentStructure::entityIdFrom, fromIdInclusive, toIdExclusive);
    }

    @Override
    protected IndexUpdater getIndexUpdater(IndexUpdateMode mode) {
        return new VectorIndexUpdater(mode.requiresIdempotency(), mode.requiresRefresh());
    }

    private class VectorIndexUpdater extends AbstractLuceneIndexUpdater {

        VectorIndexUpdater(boolean idempotent, boolean refresh) {
            super(idempotent, refresh);
        }

        @Override
        protected void addIdempotent(long entityId, Value[] values) {
            try {
                final var document = VectorDocumentStructure.createLuceneDocument(
                        entityId, (FloatingPointArray) values[0], similarityFunction);
                writer.updateOrDeleteDocument(VectorDocumentStructure.newTermForChangeOrRemove(entityId), document);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        protected void add(long entityId, Value[] values) {
            try {
                final var document = VectorDocumentStructure.createLuceneDocument(
                        entityId, (FloatingPointArray) values[0], similarityFunction);
                writer.nullableAddDocument(document);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        protected void change(long entityId, Value[] values) {
            try {
                final var term = VectorDocumentStructure.newTermForChangeOrRemove(entityId);
                final var document = VectorDocumentStructure.createLuceneDocument(
                        entityId, (FloatingPointArray) values[0], similarityFunction);
                writer.updateOrDeleteDocument(term, document);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        protected void remove(long entityId) {
            try {
                final var term = VectorDocumentStructure.newTermForChangeOrRemove(entityId);
                writer.deleteDocuments(term);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
