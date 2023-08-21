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
package org.neo4j.io.pagecache.impl.muninn;

import static org.neo4j.io.pagecache.stress.Conditions.numberOfEvictions;

import java.nio.file.OpenOption;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.stress.Condition;
import org.neo4j.io.pagecache.stress.PageCacheStressTest;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

/**
 * A stress test for Muninn page cache.
 *
 * Uses @PageCacheStressTest - see details there.
 *
 * Configured to run until it sees 100 000 evictions, which should take few seconds.
 */
@TestDirectoryExtension
class MuninnPageCacheStressIT {
    @Inject
    TestDirectory testDirectory;

    @Test
    void shouldHandleTheStressOfManyManyEvictions() throws Exception {
        runTest(Sets.immutable.empty());
    }

    @Test
    void shouldHandleTheStressOfManyManyEvictionsMultiversion() throws Exception {
        runTest(Sets.immutable.of(PageCacheOpenOptions.MULTI_VERSIONED));
    }

    private void runTest(ImmutableSet<OpenOption> openOptions) throws Exception {
        DefaultPageCacheTracer monitor = new DefaultPageCacheTracer();
        Condition condition = numberOfEvictions(monitor, 100_000);

        PageCacheStressTest runner = new PageCacheStressTest.Builder()
                .withWorkingDirectory(testDirectory.homePath())
                .with(monitor)
                .with(condition)
                .with(openOptions)
                .build();

        runner.run();
    }
}
