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
package org.neo4j.helpers.collection;

import java.util.Iterator;
import java.util.function.Function;

class MapIterable<FROM, TO> implements Iterable<TO>
{
    private final Iterable<FROM> from;
    private final Function<? super FROM,? extends TO> function;

    MapIterable( Iterable<FROM> from, Function<? super FROM,? extends TO> function )
    {
        this.from = from;
        this.function = function;
    }

    @Override
    public Iterator<TO> iterator()
    {
        return new MapIterator<>( from.iterator(), function );
    }

    static class MapIterator<FROM, TO>
            implements Iterator<TO>
    {
        private final Iterator<FROM> fromIterator;
        private final Function<? super FROM,? extends TO> function;

        MapIterator( Iterator<FROM> fromIterator, Function<? super FROM,? extends TO> function )
        {
            this.fromIterator = fromIterator;
            this.function = function;
        }

        @Override
        public boolean hasNext()
        {
            return fromIterator.hasNext();
        }

        @Override
        public TO next()
        {
            FROM from = fromIterator.next();

            return function.apply( from );
        }

        @Override
        public void remove()
        {
            fromIterator.remove();
        }
    }
}
