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

import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;

import org.junit.jupiter.api.Nested;
import org.neo4j.annotations.documented.ReporterFactories;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;

/**
 * There are a couple of very specialised indexes that cannot be tested using {@link PropertyIndexProviderCompatibilityTestSuite},
 * which is designed for testing general-purpose (supporting all types, operations and being used as a constraints ) property indexes.
 */
abstract class SpecialisedIndexProviderCompatibilityTestSuite extends IndexProviderCompatibilityTestSuite {
    @Nested
    class SpecialisedIndexPopulator extends SpecialisedIndexPopulatorCompatibility {
        SpecialisedIndexPopulator() {
            super(SpecialisedIndexProviderCompatibilityTestSuite.this);
        }
    }

    @Override
    void consistencyCheck(IndexPopulator populator) {
        ((ConsistencyCheckable) populator)
                .consistencyCheck(
                        ReporterFactories.throwingReporterFactory(),
                        NULL_CONTEXT_FACTORY,
                        Runtime.getRuntime().availableProcessors());
    }

    abstract static class Compatibility extends IndexProviderCompatabilityTestBase {
        final SpecialisedIndexProviderCompatibilityTestSuite testSuite;

        Compatibility(SpecialisedIndexProviderCompatibilityTestSuite testSuite, IndexPrototype prototype) {
            super(testSuite, prototype);
            this.testSuite = testSuite;
        }
    }
}
