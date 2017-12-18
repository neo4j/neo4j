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

import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexProgressor.NodeLabelClient;

import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

class NodeLabelIndexCursor extends IndexCursor
        implements org.neo4j.internal.kernel.api.NodeLabelIndexCursor, NodeLabelClient
{
    private Read read;
    private long node;
    private LabelSet labels;

    NodeLabelIndexCursor()
    {
        node = NO_ID;
    }

    @Override
    public void initialize( IndexProgressor progressor, boolean providesLabels )
    {
        super.initialize( progressor );
    }

    @Override
    public boolean acceptNode( long reference, LabelSet labels )
    {
        this.node = reference;
        this.labels = labels;
        return true;
    }

    public void setRead( Read read )
    {
        this.read = read;
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
    public LabelSet labels()
    {
        return labels;
    }

    @Override
    public void close()
    {
        super.close();
        node = NO_ID;
        labels = null;
        read = null;
    }

    @Override
    public boolean isClosed()
    {
        return super.isClosed();
    }

    @Override
    public String toString()
    {
        if ( isClosed() )
        {
            return "NodeLabelIndexCursor[closed state]";
        }
        else
        {
            return "NodeLabelIndexCursor[node=" + node + ", labels= " + labels +
                    ", underlying record=" + super.toString() + " ]";
        }
    }
}
