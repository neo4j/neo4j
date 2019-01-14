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
package org.neo4j.values.storable;

import org.neo4j.hashing.HashFunction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.ValueMapper;

/**
 * Not a value.
 *
 * The NULL object of the Value world. Is implemented as a singleton, to allow direct reference equality checks (==),
 * and avoid unnecessary object creation.
 */
final class NoValue extends Value
{
    @SuppressWarnings( "WeakerAccess" )
    static final NoValue NO_VALUE = new NoValue();

    private NoValue()
    {
    }

    @Override
    public boolean eq( Object other )
    {
        return this == other;
    }

    @Override
    public Boolean ternaryEquals( AnyValue other )
    {
        return null;
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return mapper.mapNoValue();
    }

    @Override
    public long updateHash( HashFunction hashFunction, long hash )
    {
        return hashFunction.update( hash, hashCode() );
    }

    @Override
    public int computeHash()
    {
        return System.identityHashCode( this );
    }

    @Override
    public boolean equals( Value other )
    {
        return this == other;
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        writer.writeNull();
    }

    @Override
    public Object asObjectCopy()
    {
        return null;
    }

    @Override
    public String toString()
    {
        return prettyPrint();
    }

    @Override
    public String prettyPrint()
    {
        return getTypeName();
    }

    @Override
    public String getTypeName()
    {
        return "NO_VALUE";
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.NO_VALUE;
    }

    @Override
    public NumberType numberType()
    {
        return NumberType.NO_NUMBER;
    }

    @Override
    int unsafeCompareTo( Value other )
    {
        return 0;
    }
}
