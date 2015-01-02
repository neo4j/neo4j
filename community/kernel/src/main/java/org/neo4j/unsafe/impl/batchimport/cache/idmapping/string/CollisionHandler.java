/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import static java.lang.String.format;

/**
 * When finding matches in {@link Encoder encoded} values that may have collisions, i.e. multiple
 * (same or different) input ids being encoded into the same value a {@link CollisionHandler} is the strategy
 * in which these collisions are handled.
 */
interface CollisionHandler
{
    public static final long COLLISION_MARK = -2L;

    long handle( long previousIndex, long foundIndex, IdGroup idGroup );

    /**
     * Capable of detecting the fact that there is a collision.
     */
    public static final CollisionHandler DETECTOR = new CollisionHandler()
    {
        @Override
        public long handle( long previousIndex, long foundIndex, IdGroup idGroup )
        {
            return previousIndex != -1
                    ? COLLISION_MARK // this is not the first one, therefore mark as conflict
                    : foundIndex;    // this is the first one
        }
    };

    /**
     * Gathers information about all entries involved in a collision, once a collision is detected.
     */
    public static class Detective implements CollisionHandler
    {
        private final Object inputId;
        private final StringBuilder error = new StringBuilder();

        public Detective( Object inputId )
        {
            this.inputId = inputId;
        }

        @Override
        public long handle( long previousIndex, long foundIndex, IdGroup idGroup )
        {
            error.append( format( "%n  %s at %d", idGroup, idGroup.translate( foundIndex ) ) );
            return foundIndex;
        }

        public IllegalStateException exception()
        {
            throw new IllegalStateException( "Id '" + inputId + "' found in multiple groups: " + error );
        }
    }
}
