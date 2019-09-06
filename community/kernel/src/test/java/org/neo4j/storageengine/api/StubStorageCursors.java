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
package org.neo4j.storageengine.api;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static java.lang.Math.toIntExact;
import static java.util.Collections.emptyIterator;
import static org.apache.commons.lang3.ArrayUtils.contains;
import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.internal.helpers.collection.MapUtil.genericMap;
import static org.neo4j.token.api.TokenHolder.TYPE_PROPERTY_KEY;

/**
 * Implementation of {@link StorageReader} with focus on making testing the storage read cursors easy without resorting to mocking.
 */
public class StubStorageCursors implements StorageReader
{
    private static final long NO_ID = -1;

    private final AtomicLong nextPropertyId = new AtomicLong();
    private final AtomicLong nextTokenId = new AtomicLong();
    private final TokenHolder propertyKeyTokenHolder = new DelegatingTokenHolder(
            ( name, internal ) -> toIntExact( nextTokenId.getAndIncrement() ), TYPE_PROPERTY_KEY );

    private final Map<Long,NodeData> nodeData = new HashMap<>();
    private final Map<String,Long> labelByName = new HashMap<>();
    private final Map<Long,String> labelById = new HashMap<>();
    private final Map<String,Long> propertyKeyByName = new HashMap<>();
    private final Map<Long,String> propertyKeyById = new HashMap<>();
    private final Map<String,Long> relationshipTypeByName = new HashMap<>();
    private final Map<Long,String> relationshipTypeById = new HashMap<>();
    private final Map<Long,PropertyData> propertyData = new HashMap<>();
    private final Map<Long,RelationshipData> relationshipData = new HashMap<>();

    public NodeData withNode( long id )
    {
        NodeData node = new NodeData( id );
        nodeData.put( id, node );
        return node;
    }

    public RelationshipData withRelationship( long id, long startNode, int type, long endNode )
    {
        RelationshipData data = new RelationshipData( id, startNode, type, endNode );
        relationshipData.put( id, data );
        return data;
    }

    private long propertyIdOf( Map<String,Value> properties )
    {
        if ( properties.isEmpty() )
        {
            return NO_ID;
        }
        long propertyId = nextPropertyId.incrementAndGet();
        propertyData.put( propertyId, new PropertyData( properties ) );
        for ( String key : properties.keySet() )
        {
            silentGetOrCreatePropertyKey( key );
        }
        return propertyId;
    }

    private int silentGetOrCreatePropertyKey( String key )
    {
        try
        {
            return propertyKeyTokenHolder.getOrCreateId( key );
        }
        catch ( KernelException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void close()
    {
    }

    public TokenHolder propertyKeyTokenHolder()
    {
        return propertyKeyTokenHolder;
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( int labelId )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public IndexDescriptor indexGetForName( String name )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public boolean indexExists( IndexDescriptor index )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public ConstraintDescriptor constraintGetForName( String name )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Collection<IndexDescriptor> indexesGetRelated( long[] labels, int propertyKeyId, EntityType entityType )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Collection<IndexDescriptor> indexesGetRelated( long[] labels, int[] propertyKeyIds, EntityType entityType )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Collection<IndexBackedConstraintDescriptor> uniquenessConstraintsGetRelated( long[] labels, int propertyKeyId, EntityType entityType )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Collection<IndexBackedConstraintDescriptor> uniquenessConstraintsGetRelated( long[] labels, int[] propertyKeyIds, EntityType entityType )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public boolean hasRelatedSchema( long[] labels, int propertyKey, EntityType entityType )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public boolean hasRelatedSchema( int label, EntityType entityType )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForRelationshipType( int relationshipType )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForSchema( SchemaDescriptor descriptor )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public boolean constraintExists( ConstraintDescriptor descriptor )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel( int labelId )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( int typeId )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAll()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Iterator<IndexDescriptor> indexGetForSchema( SchemaDescriptor descriptor )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public long countsForNode( int labelId )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public long countsForRelationship( int startLabelId, int typeId, int endLabelId )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public long nodesGetCount()
    {
        return nodeData.size();
    }

    @Override
    public long relationshipsGetCount()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public int labelCount()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public int propertyKeyCount()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public int relationshipTypeCount()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public boolean nodeExists( long id )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public boolean relationshipExists( long id )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public <T> T getOrCreateSchemaDependantState( Class<T> type, Function<StorageReader,T> factory )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public AllNodeScan allNodeScan()
    {
        throw new UnsupportedOperationException( "not implemented yet" );
    }

    @Override
    public AllRelationshipsScan allRelationshipScan()
    {
        throw new UnsupportedOperationException( "not implemented yet" );
    }

    @Override
    public StorageNodeCursor allocateNodeCursor()
    {
        return new StubStorageNodeCursor();
    }

    @Override
    public StoragePropertyCursor allocatePropertyCursor()
    {
        return new StubStoragePropertyCursor();
    }

    @Override
    public StorageRelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public StorageRelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public StorageRelationshipScanCursor allocateRelationshipScanCursor()
    {
        return new StubStorageRelationshipScanCursor();
    }

    @Override
    public StorageSchemaReader schemaSnapshot()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    public class Data<SELF>
    {
        boolean inUse = true;

        public SELF inUse( boolean inUse )
        {
            this.inUse = inUse;
            return (SELF) this;
        }
    }

    class EntityData<SELF> extends Data<SELF>
    {
        long propertyId = Read.NO_ID;

        public SELF propertyId( long propertyId )
        {
            this.propertyId = propertyId;
            return (SELF) this;
        }

        public SELF properties( Map<String,Value> properties )
        {
            return propertyId( propertyIdOf( properties ) );
        }

        public SELF properties( Object... properties )
        {
            return properties( genericMap( properties ) );
        }
    }

    public class NodeData extends EntityData<NodeData>
    {
        private final long id;
        private long[] labels = EMPTY_LONG_ARRAY;
        private long firstRelationship = NO_ID;

        NodeData( long id )
        {
            this.id = id;
        }

        public NodeData labels( long... labels )
        {
            this.labels = labels;
            return this;
        }

        public NodeData relationship( long firstRelationship )
        {
            this.firstRelationship = firstRelationship;
            return this;
        }
    }

    public class RelationshipData extends EntityData<RelationshipData>
    {
        private final long id;
        private final long startNode;
        private final int type;
        private final long endNode;

        RelationshipData( long id, long startNode, int type, long endNode )
        {
            this.id = id;
            this.startNode = startNode;
            this.type = type;
            this.endNode = endNode;
        }
    }

    private static class PropertyData
    {
        private final Map<String,Value> properties;

        PropertyData( Map<String,Value> properties )
        {
            this.properties = properties;
        }
    }

    private class StubStorageNodeCursor implements StorageNodeCursor
    {
        private long next;
        private NodeData current;
        private Iterator<Long> iterator;

        @Override
        public void scan()
        {
            this.iterator = nodeData.keySet().iterator();
            this.current = null;
        }

        @Override
        public void single( long reference )
        {
            this.iterator = null;
            this.next = reference;
        }

        @Override
        public boolean scanBatch( AllNodeScan scan, int sizeHint )
        {
            throw new UnsupportedOperationException(  );
        }

        @Override
        public long entityReference()
        {
            return current.id;
        }

        @Override
        public long[] labels()
        {
            return current.labels;
        }

        @Override
        public boolean hasLabel( int label )
        {
            return contains( current.labels, label );
        }

        @Override
        public boolean hasProperties()
        {
            return current.propertyId != NO_ID;
        }

        @Override
        public long relationshipGroupReference()
        {
            return current.firstRelationship;
        }

        @Override
        public long allRelationshipsReference()
        {
            return current.firstRelationship;
        }

        @Override
        public long propertiesReference()
        {
            return current.propertyId;
        }

        @Override
        public void properties( StoragePropertyCursor propertyCursor )
        {
            propertyCursor.initNodeProperties( propertiesReference() );
        }

        @Override
        public boolean next()
        {
            if ( iterator != null )
            {
                // scan
                while ( iterator.hasNext() )
                {
                    current = nodeData.get( iterator.next() );
                    if ( current.inUse )
                    {
                        return true;
                    }
                }
                current = null;
                return false;
            }
            else
            {
                if ( next != NO_ID )
                {
                    current = nodeData.get( next );
                    next = NO_ID;
                    return current != null && current.inUse;
                }
            }
            return false;
        }

        @Override
        public void reset()
        {
            iterator = null;
            current = null;
        }

        @Override
        public boolean isDense()
        {
            return false;
        }

        @Override
        public void close()
        {
            reset();
        }
    }

    private class StubStorageRelationshipScanCursor implements StorageRelationshipScanCursor
    {
        private Iterator<Long> iterator;
        private RelationshipData current;
        private long next;

        @Override
        public void scan()
        {
            scan( -1 );
        }

        @Override
        public void scan( int type )
        {
            iterator = relationshipData.keySet().iterator();
            next = NO_ID;
        }

        @Override
        public void single( long reference )
        {
            iterator = null;
            next = reference;
        }

        @Override
        public boolean scanBatch( AllRelationshipsScan scan, int sizeHint )
        {
            throw new UnsupportedOperationException(  );
        }

        @Override
        public long entityReference()
        {
            return current.id;
        }

        @Override
        public int type()
        {
            return current.type;
        }

        @Override
        public boolean hasProperties()
        {
            return current.propertyId != NO_ID;
        }

        @Override
        public long sourceNodeReference()
        {
            return current.startNode;
        }

        @Override
        public long targetNodeReference()
        {
            return current.endNode;
        }

        @Override
        public long propertiesReference()
        {
            return current.propertyId;
        }

        @Override
        public void properties( StoragePropertyCursor propertyCursor )
        {
            propertyCursor.initRelationshipProperties( propertiesReference() );
        }

        @Override
        public boolean next()
        {
            if ( iterator != null )
            {
                if ( !iterator.hasNext() )
                {
                    return false;
                }
                next = iterator.next();
            }

            if ( next != NO_ID )
            {
                current = relationshipData.get( next );
                next = NO_ID;
                return true;
            }
            return false;
        }

        @Override
        public void reset()
        {
            current = null;
            next = NO_ID;
        }

        @Override
        public void close()
        {
            reset();
        }
    }

    private class StubStoragePropertyCursor implements StoragePropertyCursor
    {
        private Map.Entry<String,Value> current;
        private Iterator<Map.Entry<String,Value>> iterator;

        @Override
        public void initNodeProperties( long reference )
        {
            init( reference );
        }

        @Override
        public void initRelationshipProperties( long reference )
        {
            init( reference );
        }

        private void init( long reference )
        {
            PropertyData properties = StubStorageCursors.this.propertyData.get( reference );
            iterator = properties != null ? properties.properties.entrySet().iterator() : emptyIterator();
        }

        @Override
        public void close()
        {
        }

        @Override
        public int propertyKey()
        {
            return silentGetOrCreatePropertyKey( current.getKey() );
        }

        @Override
        public ValueGroup propertyType()
        {
            return current.getValue().valueGroup();
        }

        @Override
        public Value propertyValue()
        {
            return current.getValue();
        }

        @Override
        public void reset()
        {
        }

        @Override
        public boolean next()
        {
            if ( iterator.hasNext() )
            {
                current = iterator.next();
                return true;
            }
            return false;
        }
    }
}
