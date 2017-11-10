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
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexProgressor.NodeValueClient;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

class NodeValueIndexCursor extends IndexCursor
        implements org.neo4j.internal.kernel.api.NodeValueIndexCursor, NodeValueClient
{
    private final Read read;
    private long node;
    private int[] keys;
    private Value[] values;

    NodeValueIndexCursor( Read read )
    {
        this.read = read;
    }

    @Override
    public void initialize( IndexProgressor progressor, int[] propertyIds )
    {
        super.initialize( progressor );
        this.keys = propertyIds;
    }

    @Override
    public boolean acceptNode( long reference, Value[] values )
    {
        this.node = reference;
        this.values = values;
        return true;
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
    public void close()
    {
        super.close();
        this.node = NO_ID;
        this.keys = null;
        this.values = null;
    }
}
