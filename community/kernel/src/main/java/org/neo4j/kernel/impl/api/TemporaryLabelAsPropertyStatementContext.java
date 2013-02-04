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
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.kernel.impl.api.LabelAsPropertyData.representsLabel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.core.KeyNotFoundException;
import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.core.PropertyIndexManager;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule.Kind;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.util.ArrayMap;

// TODO This is the hack where we temporarily store the labels in the property store
public class TemporaryLabelAsPropertyStatementContext implements StatementContext
{
    private static final String LABEL_PREFIX = "___label___";

    private final PropertyIndexManager propertyIndexManager;
    private final PersistenceManager persistenceManager;
    private final NeoStore neoStore;

    public TemporaryLabelAsPropertyStatementContext( PropertyIndexManager propertyIndexManager,
            PersistenceManager persistenceManager, NeoStore neoStore )
    {
        if ( neoStore == null )
            throw new AssertionError();
        this.propertyIndexManager = propertyIndexManager;
        this.persistenceManager = persistenceManager;
        this.neoStore = neoStore;
    }

    @Override
    public long getOrCreateLabelId( String label ) throws ConstraintViolationKernelException
    {
        try
        {
            return propertyIndexManager.getOrCreateId( toInternalLabelName( label ) );
        }
        catch ( TransactionFailureException e )
        {
            // Temporary workaround for the property store based label
            // implementation. Actual
            // implementation should not depend on internal kernel exception
            // messages like this.
            if ( e.getCause() != null && e.getCause() instanceof UnderlyingStorageException
                    && e.getCause().getMessage().equals( "Id capacity exceeded" ) )
            {
                throw new ConstraintViolationKernelException(
                        "The maximum number of labels available has been reached, cannot create more labels.", e );
            }
            else
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
    public boolean addLabelToNode( long labelId, long nodeId )
    {
        PropertyIndex propertyIndex = propertyIndexManager.getKeyByIdOrNull( (int) labelId );
        if ( !isLabelSetOnNode( labelId, nodeId ) )
        {
            persistenceManager.nodeAddProperty( nodeId, propertyIndex, new LabelAsProperty( nodeId ) );
            return true;
        }
        return false;
    }

    @Override
    public boolean isLabelSetOnNode( long labelId, long nodeId )
    {
        try
        {
            return getExistingPropertyData( labelId, nodeId ) != null;
        }
        catch ( InvalidRecordException e )
        {
            return false;
        }
    }

    private PropertyData getExistingPropertyData( long labelId, long nodeId )
    {
        ArrayMap<Integer, PropertyData> propertyMap = persistenceManager.loadNodeProperties( nodeId, true );
        if ( propertyMap == null )
            return null;

        PropertyData propertyData = propertyMap.get( (int) labelId );
        if ( propertyData == null )
            return null;

        ensureIsLabel( propertyData );
        return propertyData;
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
            if ( representsLabel( data ) )
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
    public boolean removeLabelFromNode( long labelId, long nodeId )
    {
        PropertyData data = getExistingPropertyData( labelId, nodeId );
        if ( data == null )
            return false;
        
        persistenceManager.nodeRemoveProperty( nodeId, data );
        return true;
    }
    
    private static class LabelPropertyBlockToNodeIterator extends PrefetchingIterator<Long>
    {
        private final Iterator<PropertyBlock> blocks;
        private final long matchingLabelId;

        LabelPropertyBlockToNodeIterator( Iterator<PropertyBlock> blocks, long labelId )
        {
            this.blocks = blocks;
            this.matchingLabelId = labelId;
        }

        @Override
        protected Long fetchNextOrNull()
        {
            while ( blocks.hasNext() )
            {
                PropertyBlock block = blocks.next();
                if ( block.getType() == PropertyType.LABEL && block.getKeyIndexId() == matchingLabelId )
                    return block.getValueBlocks()[1];
            }
            return null;
        }
    }
    
    @Override
    public Iterable<Long> getNodesWithLabel( final long labelId )
    {
        final PropertyStore propertyStore = neoStore.getPropertyStore();
        final long highestId = propertyStore.getHighestPossibleIdInUse();
        return new Iterable<Long>()
        {
            @Override
            public Iterator<Long> iterator()
            {
                return new PrefetchingIterator<Long>()
                {
                    private long id = 0L;
                    private Iterator<Long> blockNodes = Collections.<Long>emptyList().iterator();
                    
                    @Override
                    protected Long fetchNextOrNull()
                    {
                        if ( !blockNodes.hasNext() )
                        {
                            while ( id <= highestId )
                            {
                                PropertyRecord record = propertyStore.forceGetRecord( id++ );
                                if ( !record.inUse() )
                                    continue;

                                blockNodes = new LabelPropertyBlockToNodeIterator(
                                        record.getPropertyBlocks().iterator(), labelId );
                                if ( blockNodes.hasNext() )
                                    return blockNodes.next();
                            }
                            // scan exhausted
                            return null;
                        }
                        else
                        {
                            return blockNodes.next();
                        }
                    }
                };
            }
        };
    }

    private void ensureIsLabel( PropertyData data )
    {
        if ( !representsLabel( data ) )
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
    public void close( boolean successful )
    {
    }

    @Override
    public void addIndexRule( long labelId, String propertyKey ) throws ConstraintViolationKernelException
    {
        SchemaStore schemaStore = neoStore.getSchemaStore();
        long id = schemaStore.nextId();
        persistenceManager.createSchemaRule( new IndexRule( id, labelId, propertyKey ) );
    }

    @Override
    public Iterable<String> getIndexRules( final long labelId )
    {
        Iterable<SchemaRule> filtered = filter( new Predicate<SchemaRule>()
        {
            @Override
            public boolean accept( SchemaRule rule )
            {
                return rule.getLabel() == labelId && rule.getKind() == Kind.INDEX_RULE;
            }
        }, neoStore.getSchemaStore().loadAll() );
        return map( new Function<SchemaRule, String>()
        {
            @Override
            public String apply( SchemaRule from )
            {
                return ((IndexRule) from).getPropertyKey();
            }
        }, filtered );
    }
}
