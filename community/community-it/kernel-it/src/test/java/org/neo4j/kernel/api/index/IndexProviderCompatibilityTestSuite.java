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
package org.neo4j.kernel.api.index;

import java.nio.file.Path;
import org.junit.jupiter.api.Nested;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;

abstract class IndexProviderCompatibilityTestSuite {
    abstract IndexProvider createIndexProvider(
            PageCache pageCache, FileSystemAbstraction fs, Path graphDbDir, Config config);

    abstract IndexPrototype indexPrototype();

    abstract IndexType indexType();

    void consistencyCheck(IndexPopulator populator) {
        // no-op by default
    }

    void additionalConfig(Config.Builder configBuilder) {
        // can be overridden in sub-classes that wants to add additional Config settings.
    }

    @Nested
    class IndexConfigurationCompletion extends IndexConfigurationCompletionCompatibility {
        IndexConfigurationCompletion() {
            super(IndexProviderCompatibilityTestSuite.this);
        }
    }

    @Nested
    class ReadOnlyMinimalIndexAccessor extends MinimalIndexAccessorCompatibility.ReadOnly {
        ReadOnlyMinimalIndexAccessor() {
            super(IndexProviderCompatibilityTestSuite.this);
        }
    }

    @Nested
    class GeneralMinimalIndexAccessor extends MinimalIndexAccessorCompatibility.General {
        GeneralMinimalIndexAccessor() {
            super(IndexProviderCompatibilityTestSuite.this);
        }
    }
}
