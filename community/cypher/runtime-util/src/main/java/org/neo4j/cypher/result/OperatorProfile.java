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
package org.neo4j.cypher.result;

import org.neo4j.helpers.MathUtil;

/**
 * Profile for a operator during a query execution.
 */
public interface OperatorProfile
{
    /**
     * Time spent executing this operator.
     */
    long time();

    /**
     * Database hits caused while executing this operator. This is an approximate measure
     * of how many nodes, records and properties that have been read.
     */
    long dbHits();

    /**
     * Number of rows produced by this operator.
     */
    long rows();

    /**
     * Page cache hits while executing this operator.
     */
    long pageCacheHits();

    /**
     * Page cache misses while executing this operator.
     */
    long pageCacheMisses();

    default double pageCacheHitRatio()
    {
        return ( pageCacheHits() == NO_DATA || pageCacheMisses() == NO_DATA ) ?
               NO_DATA : MathUtil.portion( pageCacheHits(), pageCacheMisses() );
    }

    long NO_DATA = -1L;

    OperatorProfile NONE = new OperatorProfile()
    {
        @Override
        public long time()
        {
            return -1;
        }

        @Override
        public long dbHits()
        {
            return -1;
        }

        @Override
        public long rows()
        {
            return -1;
        }

        @Override
        public long pageCacheHits()
        {
            return -1;
        }

        @Override
        public long pageCacheMisses()
        {
            return -1;
        }
    };
}
