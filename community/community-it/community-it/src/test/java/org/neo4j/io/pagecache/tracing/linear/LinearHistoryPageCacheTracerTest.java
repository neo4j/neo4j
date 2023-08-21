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
package org.neo4j.io.pagecache.tracing.linear;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.randomharness.RandomPageCacheTestHarness;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
public class LinearHistoryPageCacheTracerTest {
    @Inject
    private TestDirectory testDirectory;

    @Disabled("This test is only here for checking that the output from the LinearHistoryPageCacheTracer looks good. "
            + "This is pretty subjective and requires manual inspection. Therefore there's no point in running it "
            + "automatically in all our builds. Instead, run it as needed when you make changes to the printout code.")
    @Test
    void makeSomeTestOutput() throws Exception {
        LinearTracers linearTracers = LinearHistoryTracerFactory.pageCacheTracer();
        try (RandomPageCacheTestHarness harness = new RandomPageCacheTestHarness()) {
            harness.setUseAdversarialIO(true);
            harness.setBasePath(testDirectory.directory("makeSomeTestOutput"));
            harness.setTracer(linearTracers.getPageCacheTracer());
            harness.setCommandCount(100);
            harness.setConcurrencyLevel(2);
            harness.setPreparation((pageCache, fs, files) -> linearTracers.processHistory(hEvent -> {}));

            harness.run(1, TimeUnit.MINUTES);

            linearTracers.printHistory(System.out);
        }
    }
}
