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

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipExplicitIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.storageengine.api.schema.IndexProgressor.ExplicitClient;

import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

class DefaultRelationshipExplicitIndexCursor extends IndexCursor<ExplicitIndexProgressor>
        implements RelationshipExplicitIndexCursor, ExplicitClient
{
    private Read read;
    private int expectedSize;
    private long relationship;
    private float score;
    private final DefaultRelationshipScanCursor scanCursor;

    private final DefaultCursors pool;

    DefaultRelationshipExplicitIndexCursor( DefaultRelationshipScanCursor scanCursor,
            DefaultCursors pool )
    {
        this.scanCursor = scanCursor;
        this.pool = pool;
    }

    @Override
    public void initialize( ExplicitIndexProgressor progressor, int expectedSize )
    {
        super.initialize( progressor );
        this.expectedSize = expectedSize;
    }

    @Override
    public boolean acceptEntity( long reference, float score )
    {
        this.relationship = reference;
        read.singleRelationship( reference, scanCursor );
        this.score = score;
        return true;
    }

    @Override
    public boolean next()
    {
        return innerNext();
    }

    public void setRead( Read read )
    {
        this.read = read;
    }

    @Override
    public int expectedTotalNumberOfResults()
    {
        return expectedSize;
    }

    @Override
    public float score()
    {
        return score;
    }

    @Override
    public void relationship( RelationshipScanCursor cursor )
    {
        read.singleRelationship( relationship, cursor );
    }

    @Override
    public void sourceNode( NodeCursor cursor )
    {
        read.singleNode( sourceNodeReference(), cursor );
    }

    @Override
    public void targetNode( NodeCursor cursor )
    {
        read.singleNode( targetNodeReference(), cursor );
    }

    @Override
    public int type()
    {
        return scanCursor.type();
    }

    @Override
    public long sourceNodeReference()
    {
        return scanCursor.sourceNodeReference();
    }

    @Override
    public long targetNodeReference()
    {
        return scanCursor.targetNodeReference();
    }

    @Override
    public long relationshipReference()
    {
        return relationship;
    }

    @Override
    public void close()
    {
        if ( !isClosed() )
        {
            super.close();
            relationship = NO_ID;
            score = 0;
            expectedSize = 0;
            read = null;

            pool.accept( this );
        }
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
            return "RelationshipExplicitIndexCursor[closed state]";
        }
        else
        {
            return "RelationshipExplicitIndexCursor[relationship=" + relationship + ", expectedSize=" + expectedSize + ", score=" + score +
                    " ,underlying record=" + super.toString() + " ]";
        }
    }

    public void release()
    {
        scanCursor.release();
    }
}
