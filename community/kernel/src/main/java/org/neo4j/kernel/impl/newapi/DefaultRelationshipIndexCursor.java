/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.values.storable.Value;

import static org.neo4j.kernel.impl.newapi.Read.NO_ID;

final class DefaultRelationshipIndexCursor extends IndexCursor<IndexProgressor> implements RelationshipIndexCursor, EntityIndexSeekClient
{
    private final CursorPool<DefaultRelationshipIndexCursor> pool;
    private final DefaultRelationshipScanCursor relationshipScanCursor;
    private Read read;
    private long relationship;
    private float score;
    private AccessMode accessMode;
    private int[] propertyIds;
    private boolean shortcutSecurity;

    DefaultRelationshipIndexCursor( CursorPool<DefaultRelationshipIndexCursor> pool, DefaultRelationshipScanCursor relationshipScanCursor )
    {
        this.pool = pool;
        this.relationshipScanCursor = relationshipScanCursor;
        relationship = NO_ID;
        score = Float.NaN;
    }

    @Override
    public void setRead( Read read )
    {
        this.read = read;
    }

    @Override
    public void initialize( IndexDescriptor descriptor, IndexProgressor progressor, IndexQuery[] query, IndexOrder indexOrder, boolean needsValues,
            boolean indexIncludesTransactionState )
    {
        assert query != null && query.length > 0;
        super.initialize( progressor );

        if ( indexOrder != IndexOrder.NONE )
        {
            throw new IllegalArgumentException( "The relationship index cursor does not yet support index orders other than IndexOrder.NONE, " +
                    "but IndexOrder." + indexOrder + " was requested." );
        }

        if ( needsValues )
        {
            throw new IllegalArgumentException( "This relationship index cursor was told to fetch the values of the index entries, but this functionality " +
                    "has not been implemented." );
        }

        if ( !indexIncludesTransactionState && read.hasTxStateWithChanges() )
        {
            SilentTokenNameLookup tokenNameLookup = new SilentTokenNameLookup( read.ktx.tokenRead() );
            String index = descriptor.userDescription( tokenNameLookup );
            throw new IllegalStateException( "There is transaction state in this transaction, and the index (" + index + ") does not take transaction " +
                    "state into account. This means that the relationship index cursor has to account for the transaction state, but this has not been " +
                    "implemented." );
        }

        shortcutSecurity = setupSecurity( descriptor, query );
    }

    @Override
    public boolean acceptEntity( long reference, float score, Value... values )
    {
        if ( !allows( reference ) )
        {
            return false;
        }
        this.relationship = reference;
        this.score = score;
        return true;
    }

    private boolean allows( long reference )
    {
        if ( shortcutSecurity )
        {
            return true;
        }

        read.singleRelationship( reference, relationshipScanCursor );
        if ( !relationshipScanCursor.next() )
        {
            return false;
        }

        int relType = relationshipScanCursor.type();
        for ( int prop : propertyIds )
        {
            if ( !accessMode.allowsReadRelationshipProperty( () -> relType, prop ) )
            {
                return false;
            }
        }
        return true;
    }

    /**
     * If the current user is allowed to traverse all relationships  nodes and read the properties no matter what label
     * the node has, we can skip checking on every node we get back.
     */
    private boolean setupSecurity( IndexDescriptor descriptor, IndexQuery[] query )
    {
        if ( accessMode == null )
        {
            accessMode = read.ktx.securityContext().mode();
        }
        propertyIds = descriptor.schema().getPropertyIds();

        for ( int relType : descriptor.schema().getEntityTokenIds() )
        {
            if ( !accessMode.allowsTraverseRelType( relType ) )
            {
                return false;
            }
        }
        if ( !accessMode.allowsTraverseAllLabels() )
        {
            return false;
        }
        for ( int propId : propertyIds )
        {
            if ( !accessMode.allowsReadPropertyAllRelTypes( propId ) )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean needsValues()
    {
        return false;
    }

    @Override
    public void relationship( RelationshipScanCursor cursor )
    {
        read.singleRelationship( relationship, cursor );
    }

    @Override
    public void sourceNode( NodeCursor cursor )
    {
        throw new UnsupportedOperationException( "We have not yet implemented tracking of the relationship end nodes in the relationship index cursor." );
    }

    @Override
    public void targetNode( NodeCursor cursor )
    {
        throw new UnsupportedOperationException( "We have not yet implemented tracking of the relationship end nodes in the relationship index cursor." );
    }

    @Override
    public int type()
    {
        throw new UnsupportedOperationException( "We have not yet implemented tracking of the relationship type in the relationship index cursor." );
    }

    @Override
    public long sourceNodeReference()
    {
        throw new UnsupportedOperationException( "We have not yet implemented tracking of the relationship end nodes in the relationship index cursor." );
    }

    @Override
    public long targetNodeReference()
    {
        throw new UnsupportedOperationException( "We have not yet implemented tracking of the relationship end nodes in the relationship index cursor." );
    }

    @Override
    public long relationshipReference()
    {
        return relationship;
    }

    @Override
    public boolean next()
    {
        boolean next = super.innerNext();
        if ( tracer != null && next )
        {
           tracer.onRelationship( relationship );
        }
        return next;
    }

    @Override
    public float score()
    {
        return score;
    }

    @Override
    public void closeInternal()
    {
        if ( !isClosed() )
        {
            closeProgressor();
            this.relationship = NO_ID;
            this.score = Float.NaN;
            pool.accept( this );
        }
    }

    @Override
    public boolean isClosed()
    {
        return isProgressorClosed();
    }

    public void release()
    {
        relationshipScanCursor.close();
        relationshipScanCursor.release();
    }
}
