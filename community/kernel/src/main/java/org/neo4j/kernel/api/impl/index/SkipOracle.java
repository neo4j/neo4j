/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.api.impl.index;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public interface SkipOracle
{
    int skip();

    public final static class Factory
    {
        public static final SkipOracle FULL_SCAN_SKIP_ORACLE = constantSkipOracle( 0 );
        public static final long AVERAGE_SKIP_LIMIT = Integer.MAX_VALUE / 2;

        private Factory()
        {
        }

        public static SkipOracle constantSkipOracle( final int skip )
        {
            return new SkipOracle()
            {
                @Override
                public int skip()
                {
                    return skip;
                }
            };
        }

        public static SkipOracle averageSkipOracle( final long averageSkip )
        {
            if ( averageSkip <= AVERAGE_SKIP_LIMIT )
            {
                return averageSkip < 2 ? FULL_SCAN_SKIP_ORACLE : randomSkipOracle( 2 * (int) averageSkip );
            }
            else
            {
                throw new IllegalStateException( "Average skip is out of bounds (> 2 * Integer.MAX_VALUE): " + averageSkip );
            }
        }

        public static SkipOracle randomSkipOracle( final int maxSkip )
        {
            return new SkipOracle()
            {
                private final Random random = ThreadLocalRandom.current();

                @Override
                public int skip()
                {
                    return random.nextInt( maxSkip );
                }
            };
        }
    }
}
