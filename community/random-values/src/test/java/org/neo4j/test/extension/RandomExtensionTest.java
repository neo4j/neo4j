/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.test.extension;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith( RandomExtension.class )
class RandomExtensionTest
{
    @Inject
    RandomRule random;

    @Test
    void randomInjectionsInitialiseField()
    {
        assertNotNull( random );
    }

    @Test
    void randomIsInitialised()
    {
        assertNotNull( random.nextAlphaNumericString() );
    }

    @Nested
    class NestedRandomTest
    {
        @Inject
        RandomRule nestedRandom;

        @Test
        @RandomRule.Seed( 15 )
        void randomSeedSetupTest()
        {
            assertEquals( 15, nestedRandom.seed() );
        }

        @Test
        void randomExtensionIgnoreAssumptionFailure()
        {
            assumeTrue( false );
        }
    }
}
