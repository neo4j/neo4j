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
package org.neo4j.impl.store.prototype.records;

import org.neo4j.impl.kernel.api.EdgeTraversalCursor;
import org.neo4j.impl.kernel.api.EdgeGroupCursor;
import org.neo4j.impl.kernel.api.LabelSet;
import org.neo4j.impl.kernel.api.PropertyCursor;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;

public class NodeCursor extends NodeRecord implements org.neo4j.impl.kernel.api.NodeCursor
{
    private final NodeStore store;

    public NodeCursor( NodeStore store )
    {
        super( -1 );
        this.store = store;
    }

    @Override
    public long nodeReference()
    {
        return getId();
    }

    @Override
    public LabelSet labels()
    {
        return new Labels( NodeLabelsField.get( this, store ) );
    }

    @Override
    public boolean hasProperties()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void edges( EdgeGroupCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void outgoingEdges( EdgeGroupCursor groups, EdgeTraversalCursor edges )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void incomingEdges( EdgeGroupCursor groups, EdgeTraversalCursor edges )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void allEdges( EdgeGroupCursor groups, EdgeTraversalCursor edges )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void properties( PropertyCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long edgeGroupReference()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long propertiesReference()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean next()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean shouldRetry()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
