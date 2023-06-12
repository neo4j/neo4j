package org.neo4j.kernel.api.impl.schema.vector;

import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndexAccessor;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.values.storable.Value;

class VectorIndexAccessor extends AbstractLuceneIndexAccessor<ValueIndexReader, DatabaseIndex<ValueIndexReader>> {

    protected VectorIndexAccessor(
            DatabaseIndex<ValueIndexReader> luceneIndex,
            IndexDescriptor descriptor,
            IndexUpdateIgnoreStrategy ignoreStrategy) {
        super(luceneIndex, descriptor, ignoreStrategy);
    }

    @Override
    public BoundedIterable<Long> newAllEntriesValueReader(
            long fromIdInclusive, long toIdExclusive, CursorContext cursorContext) {
        return null;
    }

    @Override
    protected IndexUpdater getIndexUpdater(IndexUpdateMode mode) {
        return new VectorIndexUpdater(mode.requiresIdempotency(), mode.requiresRefresh());
    }

    private class VectorIndexUpdater extends AbstractLuceneIndexUpdater {
        private VectorIndexUpdater(boolean idempotent, boolean refresh) {
            super(idempotent, refresh);
        }

        @Override
        protected void addIdempotent(long entityId, Value[] values) {}

        @Override
        protected void add(long entityId, Value[] values) {}

        @Override
        protected void change(long entityId, Value[] values) {}

        @Override
        protected void remove(long entityId) {}
    }
}
