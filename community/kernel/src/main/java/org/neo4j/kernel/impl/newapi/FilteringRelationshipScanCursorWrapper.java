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

import static org.neo4j.io.IOUtils.closeAllSilently;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Predicate;
import org.neo4j.internal.kernel.api.CloseListener;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;

public class FilteringRelationshipScanCursorWrapper implements RelationshipScanCursor {
    private final RelationshipScanCursor delegate;
    private final Predicate<RelationshipScanCursor> filter;
    private final Collection<AutoCloseable> resources;

    public FilteringRelationshipScanCursorWrapper(
            RelationshipScanCursor delegate, Predicate<RelationshipScanCursor> filter) {
        this(delegate, filter, Collections.emptyList());
    }

    /**
     * @param delegate cursor to delegate to
     * @param filter filter to apply
     * @param resources additional resources to close with this cursor, i.e. resources allocated for filter
     */
    public FilteringRelationshipScanCursorWrapper(
            RelationshipScanCursor delegate,
            Predicate<RelationshipScanCursor> filter,
            Collection<AutoCloseable> resources) {
        this.delegate = delegate;
        this.filter = filter;
        this.resources = resources;
    }

    @Override
    public boolean next() {
        while (delegate.next()) {
            if (filter.test(delegate)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() {
        closeAllSilently(resources);
        delegate.close();
    }

    @Override
    public void closeInternal() {
        delegate.closeInternal();
    }

    @Override
    public boolean isClosed() {
        return delegate.isClosed();
    }

    @Override
    public void setCloseListener(CloseListener closeListener) {
        delegate.setCloseListener(closeListener);
    }

    @Override
    public void setToken(int token) {
        delegate.setToken(token);
    }

    @Override
    public int getToken() {
        return delegate.getToken();
    }

    @Override
    public void setTracer(KernelReadTracer tracer) {
        delegate.setTracer(tracer);
    }

    @Override
    public void removeTracer() {
        delegate.removeTracer();
    }

    @Override
    public long relationshipReference() {
        return delegate.relationshipReference();
    }

    @Override
    public int type() {
        return delegate.type();
    }

    @Override
    public void source(NodeCursor cursor) {
        delegate.source(cursor);
    }

    @Override
    public void target(NodeCursor cursor) {
        delegate.target(cursor);
    }

    @Override
    public void properties(PropertyCursor cursor, PropertySelection selection) {
        delegate.properties(cursor, selection);
    }

    @Override
    public long sourceNodeReference() {
        return delegate.sourceNodeReference();
    }

    @Override
    public long targetNodeReference() {
        return delegate.targetNodeReference();
    }

    @Override
    public Reference propertiesReference() {
        return delegate.propertiesReference();
    }
}
