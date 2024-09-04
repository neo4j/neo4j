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
import org.neo4j.internal.schema.AllIndexProviderDescriptors;

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
        return provider.equals(AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR.name())
                || provider.equals(AllIndexProviderDescriptors.TEXT_V2_DESCRIPTOR.name())
                || provider.equals(AllIndexProviderDescriptors.FULLTEXT_DESCRIPTOR.name())
                || provider.equals(AllIndexProviderDescriptors.VECTOR_V1_DESCRIPTOR.name())
                || provider.equals(AllIndexProviderDescriptors.VECTOR_V2_DESCRIPTOR.name());
    }
}
