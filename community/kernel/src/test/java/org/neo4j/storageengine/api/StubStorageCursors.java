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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.impl.core.DelegatingTokenHolder;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.register.Register;
import org.neo4j.storageengine.api.schema.CapableIndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static java.lang.Math.toIntExact;
import static java.util.Collections.emptyIterator;
import static org.apache.commons.lang3.ArrayUtils.contains;
import static org.neo4j.kernel.impl.core.TokenHolder.TYPE_PROPERTY_KEY;

/**
 * Implementation of {@link StorageReader} with focus on making testing the storage read cursors easy without resorting to mocking.
 */
public class StubStorageCursors implements StorageReader
{
    private static final long NO_ID = -1;

    private final AtomicLong nextPropertyId = new AtomicLong();
    private final AtomicLong nextTokenId = new AtomicLong();
    private final TokenHolder propertyKeyTokenHolder = new DelegatingTokenHolder( name -> toIntExact( nextTokenId.getAndIncrement() ), TYPE_PROPERTY_KEY );

    private final Map<Long,NodeData> nodeData = new HashMap<>();
    private final Map<String,Long> labelByName = new HashMap<>();
    private final Map<Long,String> labelById = new HashMap<>();
    private final Map<String,Long> propertyKeyByName = new HashMap<>();
    private final Map<Long,String> propertyKeyById = new HashMap<>();
    private final Map<String,Long> relationshipTypeByName = new HashMap<>();
    private final Map<Long,String> relationshipTypeById = new HashMap<>();
    private final Map<Long,PropertyData> propertyData = new HashMap<>();
    private final Map<Long,RelationshipData> relationshipData = new HashMap<>();

    public void withNode( long id )
    {
        withNode( id, new long[0] );
    }

    public void withNode( long id, long[] labels )
    {
        withNode( id, labels, Collections.emptyMap() );
    }

    public void withNode( long id, long[] labels, Map<String,Value> properties )
    {
        nodeData.put( id, new NodeData( id, labels, NO_ID, propertyIdOf( properties ) ) );
    }

    public void withRelationship( long id, long startNode, int type, long endNode )
    {
        withRelationship( id, startNode, type, endNode, Collections.emptyMap() );
    }

    public void withRelationship( long id, long startNode, int type, long endNode, Map<String,Value> properties )
    {
        relationshipData.put( id, new RelationshipData( id, startNode, type, endNode, propertyIdOf( properties ) ) );
    }

    private long propertyIdOf( Map<String,Value> properties )
    {
        if ( properties.isEmpty() )
        {
            return NO_ID;
        }
        long propertyId = nextPropertyId.incrementAndGet();
        propertyData.put( propertyId, new PropertyData( properties ) );
        properties.keySet().forEach( propertyKeyTokenHolder::getOrCreateId );
        return propertyId;
    }

    @Override
    public void acquire()
    {
    }

    @Override
    public void release()
    {
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
    public LabelScanReader getLabelScanReader()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public IndexReader getIndexReader( IndexDescriptor index )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public IndexReader getFreshIndexReader( IndexDescriptor index )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public long reserveNode()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public long reserveRelationship()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public int reserveLabelTokenId()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public int reservePropertyKeyTokenId()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public int reserveRelationshipTypeTokenId()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public long getGraphPropertyReference()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Iterator<CapableIndexDescriptor> indexesGetForLabel( int labelId )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public CapableIndexDescriptor indexGetForName( String name )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Iterator<CapableIndexDescriptor> indexesGetAll()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Iterator<CapableIndexDescriptor> indexesGetRelatedToProperty( int propertyId )
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
    public PrimitiveLongResourceIterator nodesGetForLabel( int labelId )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public CapableIndexDescriptor indexGetForSchema( SchemaDescriptor descriptor )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public InternalIndexState indexGetState( IndexDescriptor descriptor )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public IndexReference indexReference( IndexDescriptor descriptor )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( SchemaDescriptor descriptor )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public String indexGetFailure( SchemaDescriptor descriptor )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public <EXCEPTION extends Exception> void relationshipVisit( long relationshipId, RelationshipVisitor<EXCEPTION> relationshipVisitor )
            throws EntityNotFoundException, EXCEPTION
    {
        RelationshipData data = this.relationshipData.get( relationshipId );
        if ( data == null )
        {
            throw new EntityNotFoundException( EntityType.RELATIONSHIP, relationshipId );
        }
        relationshipVisitor.visit( relationshipId, data.type, data.startNode, data.endNode );
    }

    @Override
    public void releaseNode( long id )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public void releaseRelationship( long id )
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
    public long indexSize( SchemaDescriptor descriptor )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public double indexUniqueValuesPercentage( SchemaDescriptor descriptor )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public long nodesGetCount()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
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
    public Register.DoubleLongRegister indexUpdatesAndSize( SchemaDescriptor descriptor, Register.DoubleLongRegister target )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public Register.DoubleLongRegister indexSample( SchemaDescriptor descriptor, Register.DoubleLongRegister target )
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

    private static class NodeData
    {
        private final long id;
        private final long[] labels;
        private final long firstRelationship;
        private final long propertyId;

        NodeData( long id, long[] labels, long firstRelationship, long propertyId )
        {
            this.id = id;
            this.labels = labels;
            this.firstRelationship = firstRelationship;
            this.propertyId = propertyId;
        }
    }

    private static class RelationshipData
    {
        private final long id;
        private final long startNode;
        private final int type;
        private final long endNode;
        private final long propertyId;

        RelationshipData( long id, long startNode, int type, long endNode, long propertyId )
        {
            this.id = id;
            this.startNode = startNode;
            this.type = type;
            this.endNode = endNode;
            this.propertyId = propertyId;
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
        }

        @Override
        public void single( long reference )
        {
            this.iterator = null;
            this.next = reference;
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
        public boolean next()
        {
            if ( iterator != null )
            {
                // scan
                current = iterator.hasNext() ? nodeData.get( iterator.next() ) : null;
                return true;
            }
            else
            {
                if ( next != NO_ID )
                {
                    current = nodeData.get( next );
                    next = NO_ID;
                    return true;
                }
            }
            return false;
        }

        @Override
        public void setCurrent( long nodeReference )
        {
            throw new UnsupportedOperationException( "Not implemented yet" );
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
        public void visit( long relationshipId, int typeId, long startNodeId, long endNodeId )
        {
            throw new UnsupportedOperationException( "Not implemented yet" );
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
        public void init( long reference )
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
            return propertyKeyTokenHolder.getOrCreateId( current.getKey() );
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
