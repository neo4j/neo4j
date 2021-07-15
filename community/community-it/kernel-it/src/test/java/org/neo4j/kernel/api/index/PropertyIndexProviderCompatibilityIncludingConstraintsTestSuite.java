/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.index;

import org.junit.jupiter.api.Nested;

/**
 * Temporary class until we have support for creating constraints backed by range indexes.
 * When that support is added, this should be moved back into PropertyIndexProviderCompatibilityTestSuite.
 */
abstract class PropertyIndexProviderCompatibilityIncludingConstraintsTestSuite extends PropertyIndexProviderCompatibilityTestSuite
{
    @Nested
    class UniqueConstraint extends UniqueConstraintCompatibility
    {
        UniqueConstraint()
        {
            super( PropertyIndexProviderCompatibilityIncludingConstraintsTestSuite.this );
        }
    }
}
