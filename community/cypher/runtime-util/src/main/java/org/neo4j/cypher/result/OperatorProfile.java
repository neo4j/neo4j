/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.Arrays;

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

    /**
     * The maximum amount of memory that this operator held onto while executing the query.
     */
    long maxAllocatedMemory();

    long NO_DATA = -1L;

    OperatorProfile NONE = new ConstOperatorProfile( NO_DATA );
    OperatorProfile ZERO = new ConstOperatorProfile( 0 );

    class ConstOperatorProfile implements OperatorProfile
    {

        private final long time;
        private final long dbHits;
        private final long rows;
        private final long pageCacheHits;
        private final long pageCacheMisses;
        private final long maxAllocatedMemory;

        ConstOperatorProfile( long value )
        {
            this( value, value, value, value, value, value );
        }

        public ConstOperatorProfile( long time, long dbHits, long rows, long pageCacheHits, long pageCacheMisses, long maxAllocatedMemory )
        {
            this.time = time;
            this.dbHits = dbHits;
            this.rows = rows;
            this.pageCacheHits = pageCacheHits;
            this.pageCacheMisses = pageCacheMisses;
            this.maxAllocatedMemory = maxAllocatedMemory;
        }

        @Override
        public long time()
        {
            return time;
        }

        @Override
        public long dbHits()
        {
            return dbHits;
        }

        @Override
        public long rows()
        {
            return rows;
        }

        @Override
        public long pageCacheHits()
        {
            return pageCacheHits;
        }

        @Override
        public long pageCacheMisses()
        {
            return pageCacheMisses;
        }

        @Override
        public long maxAllocatedMemory()
        {
            return maxAllocatedMemory;
        }

        @Override
        public int hashCode()
        {
            return Arrays
                    .hashCode( new long[]{this.time(), this.dbHits(), this.rows(), this.pageCacheHits(), this.pageCacheMisses(), this.maxAllocatedMemory()} );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( !(o instanceof OperatorProfile) )
            {
                return false;
            }
            OperatorProfile that = (OperatorProfile) o;
            return this.time() == that.time() &&
                   this.dbHits() == that.dbHits() &&
                   this.rows() == that.rows() &&
                   this.pageCacheHits() == that.pageCacheHits() &&
                   this.pageCacheMisses() == that.pageCacheMisses() &&
                   this.maxAllocatedMemory() == that.maxAllocatedMemory();
        }

        @Override
        public String toString()
        {
            return String.format( "Operator Profile { time: %d, dbHits: %d, rows: %d, page cache hits: %d, page cache misses: %d, max allocated: %d }",
                                  this.time(),
                                  this.dbHits(),
                                  this.rows(),
                                  this.pageCacheHits(),
                                  this.pageCacheMisses(),
                                  this.maxAllocatedMemory() );
        }
    }
}
