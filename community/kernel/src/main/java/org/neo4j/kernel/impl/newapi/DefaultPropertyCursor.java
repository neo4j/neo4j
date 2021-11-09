/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.txstate.EntityState;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_ENTITY;
import static org.neo4j.kernel.impl.newapi.Read.NO_ID;
import static org.neo4j.storageengine.api.LongReference.NULL_REFERENCE;
import static org.neo4j.storageengine.api.PropertySelection.ALL_PROPERTIES;
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
    private boolean addedInTx;
    private PropertySelection selection;

    DefaultPropertyCursor( CursorPool<DefaultPropertyCursor> pool, StoragePropertyCursor storeCursor,
                           FullAccessNodeCursor securityNodeCursor, FullAccessRelationshipScanCursor securityRelCursor )
    {
        super( pool );
        this.storeCursor = storeCursor;
        this.securityNodeCursor = securityNodeCursor;
        this.securityRelCursor = securityRelCursor;
    }

    void initNode( long nodeReference, Reference reference, PropertySelection selection, Read read, AssertOpen assertOpen )
    {
        assert nodeReference != NO_ID;

        init( selection, read, assertOpen );
        this.type = NODE;
        storeCursor.initNodeProperties( reference, selection );
        this.entityReference = nodeReference;

        initializeNodeTransactionState( nodeReference, read );
    }

    void initNode( DefaultNodeCursor nodeCursor, PropertySelection selection, Read read, AssertOpen assertOpen )
    {
        entityReference = nodeCursor.nodeReference();
        assert entityReference != NO_ID;

        init( selection, read, assertOpen );
        this.type = NODE;
        this.addedInTx = nodeCursor.currentNodeIsAddedInTx();
        if ( !addedInTx )
        {
            storeCursor.initNodeProperties( nodeCursor.storeCursor, selection );
        }
        else
        {
            storeCursor.initNodeProperties( NULL_REFERENCE, ALL_PROPERTIES );
        }

        initializeNodeTransactionState( entityReference, read );
    }

    private void initializeNodeTransactionState( long nodeReference, Read read )
    {
        if ( read.hasTxStateWithChanges() )
        {
            this.propertiesState = read.txState().getNodeState( nodeReference );
            this.txStateChangedProperties = this.propertiesState.addedAndChangedProperties().iterator();
        }
        else
        {
            this.propertiesState = null;
            this.txStateChangedProperties = null;
        }
    }

    void initRelationship( long relationshipReference, Reference reference, PropertySelection selection, Read read, AssertOpen assertOpen )
    {
        assert relationshipReference != NO_ID;

        init( selection, read, assertOpen );
        storeCursor.initRelationshipProperties( reference, selection );
        this.entityReference = relationshipReference;

        initializeRelationshipTransactionState( relationshipReference, read );
    }

    void initRelationship( DefaultRelationshipCursor relationshipCursor, PropertySelection selection, Read read, AssertOpen assertOpen )
    {
        entityReference = relationshipCursor.relationshipReference();
        assert entityReference != NO_ID;

        init( selection, read, assertOpen );
        this.addedInTx = relationshipCursor.currentRelationshipIsAddedInTx();
        if ( !addedInTx )
        {
            storeCursor.initRelationshipProperties( relationshipCursor.storeCursor, selection );
        }
        else
        {
            storeCursor.initRelationshipProperties( NULL_REFERENCE, selection );
        }

        initializeRelationshipTransactionState( entityReference, read );
    }

    private void initializeRelationshipTransactionState( long relationshipReference, Read read )
    {
        // Transaction state
        if ( read.hasTxStateWithChanges() )
        {
            this.propertiesState = read.txState().getRelationshipState( relationshipReference );
            this.txStateChangedProperties = this.propertiesState.addedAndChangedProperties().iterator();
        }
        else
        {
            this.propertiesState = null;
            this.txStateChangedProperties = null;
        }
    }

    void initEmptyRelationship( Read read, AssertOpen assertOpen )
    {
        init( ALL_PROPERTIES, read, assertOpen );
        storeCursor.initRelationshipProperties( NULL_REFERENCE, ALL_PROPERTIES );
        this.entityReference = NO_SUCH_ENTITY;

        this.propertiesState = null;
        this.txStateChangedProperties = null;
    }

    private void init( PropertySelection selection, Read read, AssertOpen assertOpen )
    {
        this.selection = selection;
        this.assertOpen = assertOpen;
        this.read = read;
        this.labels = null;
        this.type = NO_TOKEN;
    }

    boolean allowed( int propertyKey )
    {
        if ( isNode() )
        {
            ensureAccessMode();
            return accessMode.allowsReadNodeProperty( this, propertyKey );
        }
        else
        {
            ensureAccessMode();
            return accessMode.allowsReadRelationshipProperty( this, propertyKey );
        }
    }

    @Override
    public boolean next()
    {
        if ( txStateChangedProperties != null )
        {
            while ( txStateChangedProperties.hasNext() )
            {
                txStateValue = txStateChangedProperties.next();
                if ( selection.test( txStateValue.propertyKeyId() ) )
                {
                    if ( tracer != null )
                    {
                        tracer.onProperty( txStateValue.propertyKeyId() );
                    }
                    return true;
                }
            }
            txStateChangedProperties = null;
            txStateValue = null;
        }

        while ( storeCursor.next() )
        {
            int propertyKey = storeCursor.propertyKey();
            boolean skip = propertiesState != null && propertiesState.isPropertyChangedOrRemoved( propertyKey );
            if ( !skip && allowed( propertyKey ) )
            {
                if ( tracer != null )
                {
                    tracer.onProperty( propertyKey );
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
