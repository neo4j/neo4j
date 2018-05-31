/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.api.store;

import org.eclipse.collections.api.iterator.LongIterator;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.kernel.impl.api.RelationshipVisitor;

public interface RelationshipIterator extends RelationshipVisitor.Home, LongIterator
{
    /**
     * Can be called to visit the data about the most recent id returned from {@link #next()}.
     */
    @Override
    <EXCEPTION extends Exception> boolean relationshipVisit( long relationshipId,
            RelationshipVisitor<EXCEPTION> visitor ) throws EXCEPTION;

    class Empty extends PrimitiveLongCollections.PrimitiveLongBaseIterator implements RelationshipIterator
    {
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
