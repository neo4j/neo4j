/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.function.Consumer;

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

    private final Consumer<DefaultRelationshipExplicitIndexCursor> pool;

    DefaultRelationshipExplicitIndexCursor( Consumer<DefaultRelationshipExplicitIndexCursor> pool )
    {
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
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void targetNode( NodeCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public int relationshipLabel()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long sourceNodeReference()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long targetNodeReference()
    {
        throw new UnsupportedOperationException( "not implemented" );
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

            if ( pool != null )
            {
                pool.accept( this );
            }
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
}
