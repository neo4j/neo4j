/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.core.KeyNotFoundException;
import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.core.PropertyIndexManager;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.util.ArrayMap;

// TODO This is the hack where we temporarily store the labels in the property store
public class TemporaryLabelAsPropertyStatementContext implements StatementContext
{
    private static final String LABEL_PREFIX = "___label___";

    private final PropertyIndexManager propertyIndexManager;
    private final PersistenceManager persistenceManager;

    public TemporaryLabelAsPropertyStatementContext( PropertyIndexManager propertyIndexManager,
                                            PersistenceManager persistenceManager )
    {
        this.propertyIndexManager = propertyIndexManager;
        this.persistenceManager = persistenceManager;
    }

    @Override
    public long getOrCreateLabelId( String label ) throws ConstraintViolationKernelException
    {
        try {
            return propertyIndexManager.getOrCreateId( toInternalLabelName( label ) );
        } catch(TransactionFailureException e)
        {
            // Temporary workaround for the property store based label implementation. Actual
            // implementation should not depend on internal kernel exception messages like this.
            if(e.getCause() != null && e.getCause() instanceof UnderlyingStorageException
               && e.getCause().getMessage().equals( "Id capacity exceeded" ))
            {
                throw new ConstraintViolationKernelException(
                        "The maximum number of labels available has been reached, cannot create more labels.", e );
            } else
            {
                throw e;
            }
        }
    }

    @Override
    public long getLabelId( String label ) throws LabelNotFoundKernelException
    {
        try
        {
            return propertyIndexManager.getIdByKeyName( toInternalLabelName( label ) );
        }
        catch ( KeyNotFoundException e )
        {
            throw new LabelNotFoundKernelException( label, e );
        }
    }

    @Override
    public void addLabelToNode( long labelId, long nodeId )
    {
        PropertyIndex propertyIndex = propertyIndexManager.getKeyByIdOrNull( (int) labelId );
        persistenceManager.nodeAddProperty( nodeId, propertyIndex, new LabelAsProperty( nodeId ) );
    }

    @Override
    public boolean isLabelSetOnNode( long labelId, long nodeId )
    {
        try
        {
            ArrayMap<Integer, PropertyData> propertyMap = persistenceManager.loadNodeProperties( nodeId, true );
            if ( propertyMap == null )
                return false;

            PropertyData propertyData = propertyMap.get( (int) labelId );
            if ( propertyData == null )
                return false;

            ensureIsLabel( propertyData );
            return true;
        }
        catch ( InvalidRecordException e )
        {
            return false;
        }
    }
    
    @Override
    public Iterable<Long> getLabelsForNode( long nodeId )
    {
        ArrayMap<Integer, PropertyData> propertyMap = persistenceManager.loadNodeProperties( nodeId, true );
        if ( propertyMap == null )
            return emptyList();
        
        // TODO just wrap propertyMap and return lazy iterable/iterator instead?
        Collection<Long> result = new ArrayList<Long>();
        for ( PropertyData data : propertyMap.values() )
        {
            if ( data instanceof LabelAsPropertyData )
            {
                result.add( (long) data.getIndex() );
            }
        }
        return result;
    }
    
    @Override
    public String getLabelName( long labelId ) throws LabelNotFoundKernelException
    {
        try
        {
            String rawKey = propertyIndexManager.getKeyById( (int) labelId ).getKey();
            return fromInternalLabelName( rawKey );
        }
        catch ( KeyNotFoundException e )
        {
            throw new LabelNotFoundKernelException( "Label by id " + labelId, e );
        }
    }

    @Override
    public void removeLabelFromNode( long labelId, long nodeId )
    {
        ArrayMap<Integer, PropertyData> propertyMap = persistenceManager.loadNodeProperties( nodeId, true );
        if ( propertyMap == null )
            return;

        PropertyData data = propertyMap.get( (int) labelId );
        if ( data == null )
            return;

        ensureIsLabel( data );
        persistenceManager.nodeRemoveProperty( nodeId, data );
    }

    private void ensureIsLabel( PropertyData data )
    {
        if ( !(data instanceof LabelAsPropertyData) )
        {
            throw new IllegalArgumentException( "Label id " + data.getIndex() + " doesn't correspond to label" );
        }
    }

    private String toInternalLabelName( String label )
    {
        return LABEL_PREFIX + label;
    }

    private String fromInternalLabelName( String label )
    {
        return label.substring( LABEL_PREFIX.length() );
    }
    
    @Override
    public void close()
    {
    }
}
