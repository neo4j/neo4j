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

import org.neo4j.values.AnyValue;
import org.neo4j.blob.Blob;
import org.neo4j.values.ValueMapper;

public class BlobArray extends NonPrimitiveArray<Blob>
{
    Blob[] _blobs;
    BlobArraySupport _support;

    BlobArray( Blob[] blobs )
    {
        this._blobs = blobs;
        _support = new BlobArraySupport( blobs );
    }

    @Override
    public Blob[] value()
    {
        return this._blobs;
    }

    @Override
    public <T> T map( ValueMapper<T> mapper )
    {
        return (T) _blobs;
    }

    @Override
    public String getTypeName()
    {
        return "BlobArray";
    }

    @Override
    public AnyValue value( int offset )
    {
        return _support.values()[offset];
    }

    @Override
    public boolean equals( Value other )
    {
        return _support._equals( other );
    }

    @Override
    int unsafeCompareTo( Value other )
    {
        return _support.unsafeCompareTo( other );
    }

    @Override
    public <E extends Exception> void writeTo( ValueWriter<E> writer ) throws E
    {
        _support.writeTo( writer );
    }

    @Override
    public ValueGroup valueGroup()
    {
        return ValueGroup.NO_VALUE;
    }
}
