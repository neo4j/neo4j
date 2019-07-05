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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.values.storable.Value;

public class ExtendedNodeValueIndexCursorAdapter implements NodeValueIndexCursor, EntityIndexSeekClient
{
    @Override
    public void close()
    {

    }

    @Override
    public boolean isClosed()
    {
        return false;
    }

    @Override
    public boolean next()
    {
        return false;
    }

    @Override
    public void node( NodeCursor cursor )
    {

    }

    @Override
    public long nodeReference()
    {
        return 0;
    }

    @Override
    public int numberOfProperties()
    {
        return 0;
    }

    @Override
    public int propertyKey( int offset )
    {
        return 0;
    }

    @Override
    public boolean hasValue()
    {
        return false;
    }

    @Override
    public Value propertyValue( int offset )
    {
        return null;
    }

    @Override
    public float score()
    {
        return 0;
    }

    @Override
    public void setRead( Read read )
    {
    }

    @Override
    public void initialize( IndexDescriptor descriptor, IndexProgressor progressor, IndexQuery[] query, IndexOrder indexOrder, boolean needsValues,
            boolean indexIncludesTransactionState )
    {
    }

    @Override
    public boolean acceptEntity( long reference, float score, Value... values )
    {
        return false;
    }

    @Override
    public boolean needsValues()
    {
        return false;
    }

    @Override
    public void setTracer( KernelReadTracer tracer )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
