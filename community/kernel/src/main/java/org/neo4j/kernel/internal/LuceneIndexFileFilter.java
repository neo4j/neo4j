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
package org.neo4j.kernel.internal;

import java.nio.file.Path;
import org.neo4j.kernel.api.impl.schema.TextIndexProvider;
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexProvider;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory;

/**
 * A filter which only matches lucene index files.
 * This class contains logic that is really index provider specific, but to ask index providers becomes tricky since
 * they aren't always available and this filter is also expected to be used in offline scenarios.
 */
public class LuceneIndexFileFilter extends IndexFileFilter {

    public LuceneIndexFileFilter(Path storeDir) {
        super(storeDir);
    }

    @Override
    public boolean test(Path path) {
        if (!super.test(path)) {
            return false;
        }

        final var provider = provider(path);
        return provider.equals(TextIndexProvider.DESCRIPTOR.name())
                || provider.equals(TrigramIndexProvider.DESCRIPTOR.name())
                || provider.equals(FulltextIndexProviderFactory.DESCRIPTOR.name())
                || provider.equals(VectorIndexVersion.V1_0.descriptor().name());
    }
}
