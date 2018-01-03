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
package org.neo4j.consistency.checking.full;

/**
 * Represents a {@link Stage} in the consistency check. A consistency check goes through multiple stages.
 */
public interface Stage
{
    boolean isParallel();

    boolean isForward();

    String getPurpose();

    int ordinal();

    int[] getCacheSlotSizes();

    class Adapter implements Stage
    {
        private final boolean parallel;
        private final boolean forward;
        private final String purpose;
        private final int[] cacheSlotSizes;

        public Adapter( boolean parallel, boolean forward, String purpose, int... cacheSlotSizes )
        {
            this.parallel = parallel;
            this.forward = forward;
            this.purpose = purpose;
            this.cacheSlotSizes = cacheSlotSizes;
        }

        @Override
        public boolean isParallel()
        {
            return parallel;
        }

        @Override
        public boolean isForward()
        {
            return forward;
        }

        @Override
        public String getPurpose()
        {
            return purpose;
        }

        @Override
        public int ordinal()
        {
            return -1;
        }

        @Override
        public int[] getCacheSlotSizes()
        {
            return cacheSlotSizes;
        }
    }

    Stage SEQUENTIAL_FORWARD = new Adapter( false, true, "General purpose" );
    Stage PARALLEL_FORWARD = new Adapter( true, true, "General purpose" );
}
