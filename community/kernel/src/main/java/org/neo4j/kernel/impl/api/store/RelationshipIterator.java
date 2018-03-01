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
package org.neo4j.kernel.impl.api.store;

import org.neo4j.collection.primitive.PrimitiveLongResourceCollections;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.kernel.impl.api.RelationshipVisitor;

public interface RelationshipIterator extends PrimitiveLongResourceIterator, RelationshipVisitor.Home
{
    /**
     * Can be called to visit the data about the most recent id returned from {@link #next()}.
     */
    @Override
    <EXCEPTION extends Exception> boolean relationshipVisit( long relationshipId,
            RelationshipVisitor<EXCEPTION> visitor ) throws EXCEPTION;

    @Override
    default void close()
    {
        // empty close
    }

    class Empty extends PrimitiveLongResourceCollections.PrimitiveLongBaseResourceIterator implements RelationshipIterator
    {
        private Empty()
        {
            super( null );
        }

        @Override
        public <EXCEPTION extends Exception> boolean relationshipVisit( long relationshipId,
                RelationshipVisitor<EXCEPTION> visitor )
        {   // Nothing to visit
            return false;
        }

        @Override
        protected boolean fetchNext()
        {
            return false;
        }
    }

    RelationshipIterator EMPTY = new Empty();

}
