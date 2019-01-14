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
package org.neo4j.values;

public abstract class AnyValue
{
    private int hash;

    // this should be final, but Mockito barfs if it is,
    // so we need to just manually ensure it isn't overridden
    @Override
    public boolean equals( Object other )
    {
        return this == other || other != null && eq( other );
    }

    @Override
    public final int hashCode()
    {
        //We will always recompute hashcode for values
        //where `hashCode == 0`, e.g. empty strings and empty lists
        //however that shouldn't be shouldn't be too costly
        if ( hash == 0 )
        {
            hash = computeHash();
        }
        return hash;
    }

    protected abstract boolean eq( Object other );

    protected abstract int computeHash();

    public abstract <E extends Exception> void writeTo( AnyValueWriter<E> writer ) throws E;

    public boolean isSequenceValue()
    {
        return false; // per default Values are no SequenceValues
    }

    public abstract Boolean ternaryEquals( AnyValue other );

    public abstract <T> T map( ValueMapper<T> mapper );

    public abstract String getTypeName();
}
