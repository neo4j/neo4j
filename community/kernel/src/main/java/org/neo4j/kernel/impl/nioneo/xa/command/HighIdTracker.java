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
package org.neo4j.kernel.impl.nioneo.xa.command;

import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;

/**
 * Tracks high ids when apply a transaction. Multiple calls to {@link #track(CommonAbstractStore, long)} can be
 * expected, to collect candidate high ids per store. Finally a call to {@link #apply()} should update the stores
 * with the candidate high ids seen in {@link #track(CommonAbstractStore, long)}.
 */
public interface HighIdTracker
{
    void track( CommonAbstractStore store, long highId );
    
    public static final HighIdTracker NO_TRACKING = new HighIdTracker()
    {
        @Override
        public void track( CommonAbstractStore store, long highId )
        {   // Don't track
        }

        @Override
        public void apply()
        {   // Don't apply anything
        }
    };

    void apply();
}
