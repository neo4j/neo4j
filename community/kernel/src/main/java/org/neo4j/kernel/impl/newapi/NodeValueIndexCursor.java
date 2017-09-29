/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

class NodeValueIndexCursor implements org.neo4j.internal.kernel.api.NodeValueIndexCursor,
        CursorProgressor.Cursor<IndexState.NodeValue>, IndexState.NodeValue
{
    private final Read read;
    private long node;
    private int[] keys;
    private Value[] values;
    private CursorProgressor<IndexState.NodeValue> progressor;

    NodeValueIndexCursor( Read read )
    {
        this.read = read;
    }

    @Override
    public void empty()
    {
        close();
    }

    @Override
    public void initialize( CursorProgressor<IndexState.NodeValue> progressor )
    {
        this.progressor = progressor;
    }

    @Override
    public void done()
    {
        close();
    }

    @Override
    public void node( long reference, int[] keys, Value[] values )
    {
        this.node = reference;
        this.keys = keys;
        this.values = values;
    }

    @Override
    public void node( NodeCursor cursor )
    {
        read.singleNode( node, cursor );
    }

    @Override
    public long nodeReference()
    {
        return node;
    }

    @Override
    public int numberOfProperties()
    {
        return keys == null ? 0 : keys.length;
    }

    @Override
    public int propertyKey( int offset )
    {
        return keys[offset];
    }

    @Override
    public Value propertyValue( int offset )
    {
        return values[offset];
    }

    @Override
    public boolean next()
    {
        return progressor != null && progressor.next( this );
    }

    @Override
    public boolean shouldRetry()
    {
        return false;
    }

    @Override
    public void close()
    {
        this.progressor = null;
        this.node = NO_ID;
        this.keys = null;
        this.values = null;
    }
}
