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
package org.neo4j.kernel.impl.index.schema;

import static org.neo4j.internal.schema.IndexCapability.NO_CAPABILITY;

import org.junit.jupiter.api.Nested;
import org.neo4j.kernel.api.impl.fulltext.FulltextIndexCapability;
import org.neo4j.kernel.api.impl.schema.TextIndexProvider;
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexProvider;

public class NoSupportPartitionedScanTest extends SupportPartitionedScanTestSuite {
    NoSupportPartitionedScanTest() {
        super(NO_CAPABILITY, NO_SUPPORT);
    }

    // point index being implemented
    @Nested
    class Point extends SupportPartitionedScanTestSuite {
        Point() {
            super(PointIndexProvider.CAPABILITY, NO_SUPPORT);
        }
    }

    // text index being implemented
    @Nested
    class Text extends SupportPartitionedScanTestSuite {
        Text() {
            super(TextIndexProvider.CAPABILITY, NO_SUPPORT);
        }
    }

    @Nested
    class Trigram extends SupportPartitionedScanTestSuite {
        Trigram() {
            super(TrigramIndexProvider.CAPABILITY, NO_SUPPORT);
        }
    }

    @Nested
    class Fulltext extends SupportPartitionedScanTestSuite {
        Fulltext() {
            super(new FulltextIndexCapability(false), NO_SUPPORT);
        }
    }
}
