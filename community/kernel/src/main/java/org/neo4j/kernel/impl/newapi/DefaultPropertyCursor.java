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

import java.util.Iterator;
import java.util.function.Supplier;

import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelTypeSupplier;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.txstate.EntityState;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_ENTITY;
import static org.neo4j.kernel.impl.newapi.Read.NO_ID;
import static org.neo4j.token.api.TokenConstants.NO_TOKEN;

public class DefaultPropertyCursor extends TraceableCursor<DefaultPropertyCursor> implements PropertyCursor, Supplier<TokenSet>, RelTypeSupplier
{
    private static final int NODE = -2;
    private Read read;
    private final StoragePropertyCursor storeCursor;
    private final FullAccessNodeCursor securityNodeCursor;
    private final FullAccessRelationshipScanCursor securityRelCursor;
    private EntityState propertiesState;
    private Iterator<StorageProperty> txStateChangedProperties;
    private StorageProperty txStateValue;
    private AssertOpen assertOpen;
    private AccessMode accessMode;
    private long entityReference = NO_ID;
    private TokenSet labels;
    //stores relationship type or NODE if not a relationship
    private int type = NO_TOKEN;

    DefaultPropertyCursor( CursorPool<DefaultPropertyCursor> pool, StoragePropertyCursor storeCursor,
                           FullAccessNodeCursor securityNodeCursor, FullAccessRelationshipScanCursor securityRelCursor )
    {
        super( pool );
        this.storeCursor = storeCursor;
        this.securityNodeCursor = securityNodeCursor;
        this.securityRelCursor = securityRelCursor;
    }

    void initNode( long nodeReference, long reference, Read read, AssertOpen assertOpen )
    {
        assert nodeReference != NO_ID;

        init( read, assertOpen );
        this.type = NODE;
        storeCursor.initNodeProperties( reference );
        this.entityReference = nodeReference;

        // Transaction state
        if ( read.hasTxStateWithChanges() )
        {
            this.propertiesState = read.txState().getNodeState( nodeReference );
            this.txStateChangedProperties = this.propertiesState.addedAndChangedProperties();
        }
        else
        {
            this.propertiesState = null;
            this.txStateChangedProperties = null;
        }
    }

    void initRelationship( long relationshipReference, long reference, Read read, AssertOpen assertOpen )
    {
        assert relationshipReference != NO_ID;

        init( read, assertOpen );
        storeCursor.initRelationshipProperties( reference );
        this.entityReference = relationshipReference;

        // Transaction state
        if ( read.hasTxStateWithChanges() )
        {
            this.propertiesState = read.txState().getRelationshipState( relationshipReference );
            this.txStateChangedProperties = this.propertiesState.addedAndChangedProperties();
        }
        else
        {
            this.propertiesState = null;
            this.txStateChangedProperties = null;
        }
    }

    void initEmptyRelationship( Read read, AssertOpen assertOpen )
    {
        init( read, assertOpen );
        storeCursor.initRelationshipProperties( NO_ID );
        this.entityReference = NO_SUCH_ENTITY;

        this.propertiesState = null;
        this.txStateChangedProperties = null;
    }

    private void init( Read read, AssertOpen assertOpen )
    {
        this.assertOpen = assertOpen;
        this.read = read;
        this.labels = null;
        this.type = NO_TOKEN;
    }

    boolean allowed()
    {
        if ( isNode() )
        {
            ensureAccessMode();
            return accessMode.allowsReadNodeProperty( this, propertyKey() );
        }
        else
        {
            ensureAccessMode();
            return accessMode.allowsReadRelationshipProperty( this, propertyKey() );
        }
    }

    @Override
    public boolean next()
    {
        if ( txStateChangedProperties != null )
        {
            if ( txStateChangedProperties.hasNext() )
            {
                txStateValue = txStateChangedProperties.next();
                if ( tracer != null )
                {
                    tracer.onProperty( propertyKey() );
                }
                return true;
            }
            else
            {
                txStateChangedProperties = null;
                txStateValue = null;
            }
        }

        while ( storeCursor.next() )
        {
            boolean skip = propertiesState != null && propertiesState.isPropertyChangedOrRemoved( storeCursor.propertyKey() );
            if ( !skip && allowed() )
            {
                if ( tracer != null )
                {
                    tracer.onProperty( propertyKey() );
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public void closeInternal()
    {
        if ( !isClosed() )
        {
            propertiesState = null;
            txStateChangedProperties = null;
            txStateValue = null;
            read = null;
            storeCursor.reset();
            accessMode = null;
        }
        super.closeInternal();
    }

    @Override
    public int propertyKey()
    {
        if ( txStateValue != null )
        {
            return txStateValue.propertyKeyId();
        }
        return storeCursor.propertyKey();
    }

    @Override
    public ValueGroup propertyType()
    {
        if ( txStateValue != null )
        {
            return txStateValue.value().valueGroup();
        }
        return storeCursor.propertyType();
    }

    @Override
    public Value propertyValue()
    {
        if ( txStateValue != null )
        {
            return txStateValue.value();
        }

        Value value = storeCursor.propertyValue();

        assertOpen.assertOpen();
        return value;
    }

    @Override
    public boolean seekProperty( int property )
    {
        if ( property == NO_TOKEN )
        {
            return false;
        }
        while ( next() )
        {
            if ( property == this.propertyKey() )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isClosed()
    {
        return read == null;
    }

    @Override
    public String toString()
    {
        if ( isClosed() )
        {
            return "PropertyCursor[closed state]";
        }
        else
        {
            return "PropertyCursor[id=" + propertyKey() +
                   ", " + storeCursor + " ]";
        }
    }

    /**
     * Gets the label while ignoring removes in the tx state. Implemented as a Supplier so that we don't need additional
     * allocations.
     *
     * Only used for security checks
     */
    @Override
    public TokenSet get()
    {
        assert isNode();

        if ( labels == null )
        {
            read.singleNode( entityReference, securityNodeCursor );
            securityNodeCursor.next();
            labels = securityNodeCursor.labelsIgnoringTxStateSetRemove();
        }
        return labels;
    }

    /**
     * Only used for security checks
     */
    @Override
    public int getRelType()
    {
        assert isRelationship();

        if ( type < 0 )
        {
            read.singleRelationship( entityReference, securityRelCursor );
            securityRelCursor.next();
            this.type = securityRelCursor.type();
        }
        return type;
    }

    private void ensureAccessMode()
    {
        if ( accessMode == null )
        {
            accessMode = read.ktx.securityContext().mode();
        }
    }

    public void release()
    {
        if ( storeCursor != null )
        {
            storeCursor.close();
        }
        if ( securityNodeCursor != null )
        {
            securityNodeCursor.close();
            securityNodeCursor.release();
        }
        if ( securityRelCursor != null )
        {
            securityRelCursor.close();
            securityRelCursor.release();
        }
    }

    private boolean isNode()
    {
        return type == NODE;
    }

    private boolean isRelationship()
    {
        return type != NODE;
    }
}
