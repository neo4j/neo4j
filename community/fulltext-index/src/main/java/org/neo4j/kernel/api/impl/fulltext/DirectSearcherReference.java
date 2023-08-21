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
package org.neo4j.kernel.api.impl.fulltext;

import java.io.Closeable;
import java.io.IOException;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.partition.Neo4jIndexSearcher;

class DirectSearcherReference implements SearcherReference {
    private final Neo4jIndexSearcher searcher;
    private final Closeable resource;

    DirectSearcherReference(Neo4jIndexSearcher searcher, Closeable resource) {
        this.searcher = searcher;
        this.resource = resource;
    }

    @Override
    public void close() throws IOException {
        resource.close();
    }

    @Override
    public Neo4jIndexSearcher getIndexSearcher() {
        return searcher;
    }
}
