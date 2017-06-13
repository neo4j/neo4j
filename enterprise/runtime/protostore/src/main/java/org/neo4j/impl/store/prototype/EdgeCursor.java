/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.store.prototype;

import org.neo4j.impl.kernel.api.EdgeDataAccessor;
import org.neo4j.impl.kernel.api.NodeCursor;
import org.neo4j.impl.kernel.api.PropertyCursor;
import org.neo4j.impl.kernel.api.Read;
import org.neo4j.impl.store.cursors.ReadCursor;

import static org.neo4j.impl.store.prototype.PropertyCursor.NO_PROPERTIES;

public abstract class EdgeCursor<Store extends Read> extends ReadCursor implements EdgeDataAccessor
{
    public static final long NO_EDGE = -1;
    protected final Store store;

    protected EdgeCursor( Store store )
    {
        this.store = store;
    }

    @Override
    public boolean hasProperties()
    {
        return propertiesReference() != NO_PROPERTIES;
    }

    @Override
    public void source( NodeCursor cursor )
    {
        store.singleNode( sourceNodeReference(), cursor );
    }

    @Override
    public void target( NodeCursor cursor )
    {
        store.singleNode( targetNodeReference(), cursor );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        store.edgeProperties( propertiesReference(), cursor );
    }
}
