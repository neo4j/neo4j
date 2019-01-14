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

import org.neo4j.collection.RawIterator;
import org.neo4j.function.ThrowingFunction;

class RawMapIterator<FROM, TO, EX extends Exception> implements RawIterator<TO,EX>
{
    private final RawIterator<FROM,EX> fromIterator;
    private final ThrowingFunction<? super FROM,? extends TO,EX> function;

    RawMapIterator( RawIterator<FROM,EX> fromIterator, ThrowingFunction<? super FROM,? extends TO,EX> function )
    {
        this.fromIterator = fromIterator;
        this.function = function;
    }

    @Override
    public boolean hasNext() throws EX
    {
        return fromIterator.hasNext();
    }

    @Override
    public TO next() throws EX
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
