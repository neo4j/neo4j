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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith( Parameterized.class )
public class LogPositionTest
{
    @Parameterized.Parameter()
    public LogPosition logPositionA;

    @Parameterized.Parameter( 1 )
    public LogPosition logPositionB;

    @Parameterized.Parameters
    public static Collection<LogPosition[]> logPositions()
    {
        return Arrays.asList( new LogPosition[]{new LogPosition( 0, 1 ), new LogPosition( 0, 0 )},
                new LogPosition[]{new LogPosition( 0, 11 ), new LogPosition( 0, 7 )},
                new LogPosition[]{new LogPosition( 2, 1 ), new LogPosition( 2, 0 )},
                new LogPosition[]{new LogPosition( 2, 17 ), new LogPosition( 2, 15 )},
                new LogPosition[]{new LogPosition( 1, 1 ), new LogPosition( 0, 1 )},
                new LogPosition[]{new LogPosition( 5, 1 ), new LogPosition( 3, 10 )},
                new LogPosition[]{new LogPosition( Integer.MAX_VALUE, Integer.MAX_VALUE + 1L ),
                        new LogPosition( Integer.MAX_VALUE, Integer.MAX_VALUE )},
                new LogPosition[]{new LogPosition( Long.MAX_VALUE, Long.MAX_VALUE ),
                        new LogPosition( Integer.MAX_VALUE + 1L, Long.MAX_VALUE )},
                new LogPosition[]{new LogPosition( Long.MAX_VALUE, Long.MAX_VALUE ),
                        new LogPosition( Long.MAX_VALUE, Long.MAX_VALUE - 1 )} );
    }

    @SuppressWarnings( {"EqualsWithItself", "SelfComparison"} )
    @Test
    public void logPositionComparison()
    {
        assertEquals( 1, logPositionA.compareTo( logPositionB ) );
        assertEquals( -1, logPositionB.compareTo( logPositionA ) );

        assertEquals( 0, logPositionA.compareTo( logPositionA ) );
        assertEquals( 0, logPositionB.compareTo( logPositionB ) );
    }
}
