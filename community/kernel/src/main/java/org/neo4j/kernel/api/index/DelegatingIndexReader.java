/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.index;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;

public class DelegatingIndexReader implements IndexReader
{
    private final IndexReader delegate;

    public DelegatingIndexReader( IndexReader delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public PrimitiveLongIterator seek( Object value )
    {
        return delegate.seek( value );
    }

    @Override
    public PrimitiveLongIterator rangeSeekByNumberInclusive( Number lower, Number upper )
    {
        return delegate.rangeSeekByNumberInclusive( lower, upper );
    }

    @Override
    public PrimitiveLongIterator rangeSeekByString( String lower, boolean includeLower,
                                                    String upper, boolean includeUpper )
    {
        return delegate.rangeSeekByString( lower, includeLower, upper, includeUpper );
    }

    @Override
    public PrimitiveLongIterator rangeSeekByPrefix( String prefix )
    {
        return delegate.rangeSeekByPrefix( prefix );
    }

    @Override
    public PrimitiveLongIterator scan()
    {
        return delegate.scan();
    }

    @Override
    public PrimitiveLongIterator containsString( String exactTerm )
    {
        return delegate.containsString( exactTerm );
    }

    @Override
    public PrimitiveLongIterator endsWith( String suffix )
    {
        return delegate.endsWith( suffix );
    }

    @Override
    public long countIndexedNodes( long nodeId, Object propertyValue )
    {
        return delegate.countIndexedNodes( nodeId, propertyValue );
    }

    @Override
    public IndexSampler createSampler()
    {
        return delegate.createSampler();
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }
}
