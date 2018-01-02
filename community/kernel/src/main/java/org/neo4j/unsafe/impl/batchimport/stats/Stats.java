/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.stats;

/**
 * Common {@link Stat} implementations.
 */
public class Stats
{
    public static abstract class LongBasedStat implements Stat
    {
        private final DetailLevel detailLevel;

        public LongBasedStat( DetailLevel detailLevel )
        {
            this.detailLevel = detailLevel;
        }

        @Override
        public DetailLevel detailLevel()
        {
            return detailLevel;
        }

        @Override
        public String toString()
        {
            return String.valueOf( asLong() );
        }
    }

    public static Stat longStat( final long stat )
    {
        return longStat( stat, DetailLevel.BASIC );
    }

    public static Stat longStat( final long stat, DetailLevel detailLevel )
    {
        return new LongBasedStat( detailLevel )
        {
            @Override
            public long asLong()
            {
                return stat;
            }
        };
    }
}
