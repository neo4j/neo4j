/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.store;

import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.TooManyLabelsException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.properties.PropertyKeyIdIterator;
import org.neo4j.kernel.impl.api.DegreeVisitor;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.util.PrimitiveLongResourceIterator;
import org.neo4j.kernel.impl.util.register.NeoRegister;
import org.neo4j.register.Register;

import static org.neo4j.collection.primitive.PrimitiveIntCollections.asArray;
import static org.neo4j.collection.primitive.PrimitiveIntCollections.iterator;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.map;

/**
 * This is the object-caching layer. It delegates to the legacy object cache system if possible, or delegates to the
 * disk layer if there is no relevant caching.
 *
 * An important consideration when working on this is that there are plans to remove the object cache, which means that
 * the aim for this layer is to disappear.
 */
public class CacheLayer implements StoreReadLayer
{
    private static final Function<? super SchemaRule, IndexDescriptor> TO_INDEX_RULE =
            new Function<SchemaRule, IndexDescriptor>()
    {
        @Override
        public IndexDescriptor apply( SchemaRule from )
        {
            IndexRule rule = (IndexRule) from;
            // We know that we only have int range of property key ids.
            return new IndexDescriptor( rule.getLabel(), rule.getPropertyKey() );
        }
    };

    /* Experimental cache_type setting for disabling this cache layer entirely. */
    public static final String EXPERIMENTAL_OFF = "experimental-off";

    private final CacheLoader<Iterator<DefinedProperty>> nodePropertyLoader = new CacheLoader<Iterator<DefinedProperty>>()
    {
        @Override
        public Iterator<DefinedProperty> load( long id ) throws EntityNotFoundException
        {
            return diskLayer.nodeGetAllProperties( id );
        }
    };
    private final CacheLoader<Iterator<DefinedProperty>> relationshipPropertyLoader = new CacheLoader<Iterator<DefinedProperty>>()
    {
        @Override
        public Iterator<DefinedProperty> load( long id ) throws EntityNotFoundException
        {
            return diskLayer.relationshipGetAllProperties( id );
        }
    };
    private final CacheLoader<Iterator<DefinedProperty>> graphPropertyLoader = new CacheLoader<Iterator<DefinedProperty>>()
    {
        @Override
        public Iterator<DefinedProperty> load( long id ) throws EntityNotFoundException
        {
            return diskLayer.graphGetAllProperties();
        }
    };
    private final CacheLoader<int[]> nodeLabelLoader = new CacheLoader<int[]>()
    {
        @Override
        public int[] load( long id ) throws EntityNotFoundException
        {
            return asArray( diskLayer.nodeGetLabels( id ) );
        }
    };

    private final PersistenceCache persistenceCache;
    private final SchemaCache schemaCache;
    private final DiskLayer diskLayer;
    private final IndexingService indexingService;

    public CacheLayer(
            DiskLayer diskLayer,
            PersistenceCache persistenceCache,
            IndexingService indexingService,
            SchemaCache schemaCache )
    {
        this.diskLayer = diskLayer;
        this.persistenceCache = persistenceCache;
        this.indexingService = indexingService;
        this.schemaCache = schemaCache;
    }

    @Override
    public boolean nodeExists( long nodeId )
    {
        return diskLayer.nodeExists( nodeId );
    }

    @Override
    public boolean nodeHasLabel( long nodeId, int labelId ) throws EntityNotFoundException
    {
        return persistenceCache.nodeHasLabel( nodeId, labelId, nodeLabelLoader );
    }

    @Override
    public PrimitiveIntIterator nodeGetLabels( long nodeId ) throws EntityNotFoundException
    {
        return iterator( persistenceCache.nodeGetLabels( nodeId, nodeLabelLoader ) );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( int labelId )
    {
        return toIndexDescriptors( schemaCache.schemaRulesForLabel( labelId ), SchemaRule.Kind.INDEX_RULE );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        return toIndexDescriptors( schemaCache.schemaRules(), SchemaRule.Kind.INDEX_RULE );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( int labelId )
    {
        return toIndexDescriptors( schemaCache.schemaRulesForLabel( labelId ),
                SchemaRule.Kind.CONSTRAINT_INDEX_RULE );
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll()
    {
        return toIndexDescriptors( schemaCache.schemaRules(), SchemaRule.Kind.CONSTRAINT_INDEX_RULE );
    }

    private static Iterator<IndexDescriptor> toIndexDescriptors( Iterable<SchemaRule> rules,
                                                                 final SchemaRule.Kind kind )
    {
        Iterator<SchemaRule> filteredRules = filter( new Predicate<SchemaRule>()
        {
            @Override
            public boolean accept( SchemaRule item )
            {
                return item.getKind() == kind;
            }
        }, rules.iterator() );
        return map( TO_INDEX_RULE, filteredRules );
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index )
            throws SchemaRuleNotFoundException
    {
        IndexRule rule = indexRule( index, SchemaStorage.IndexRuleKind.ALL );
        if ( rule != null )
        {
            return rule.getOwningConstraint();
        }
        return diskLayer.indexGetOwningUniquenessConstraintId( index );
    }

    @Override
    public long indexGetCommittedId( IndexDescriptor index, SchemaStorage.IndexRuleKind kind )
            throws SchemaRuleNotFoundException
    {
        IndexRule rule = indexRule( index, kind );
        if ( rule != null )
        {
            return rule.getId();
        }
        return diskLayer.indexGetCommittedId( index, kind );
    }

    @Override
    public IndexRule indexRule( IndexDescriptor index, SchemaStorage.IndexRuleKind kind )
    {
        for ( SchemaRule rule : schemaCache.schemaRulesForLabel( index.getLabelId() ) )
        {
            if ( rule instanceof IndexRule )
            {
                IndexRule indexRule = (IndexRule) rule;
                if ( kind.isOfKind( indexRule ) && indexRule.getPropertyKey() == index.getPropertyKeyId() )
                {
                    return indexRule;
                }
            }
        }
        return null;
    }

    @Override
    public PrimitiveLongIterator nodeGetPropertyKeys( long nodeId ) throws EntityNotFoundException
    {
        return persistenceCache.nodeGetPropertyKeys( nodeId, nodePropertyLoader );
    }

    @Override
    public Property nodeGetProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        return persistenceCache.nodeGetProperty( nodeId, propertyKeyId, nodePropertyLoader );
    }

    @Override
    public Iterator<DefinedProperty> nodeGetAllProperties( long nodeId ) throws EntityNotFoundException
    {
        return persistenceCache.nodeGetProperties( nodeId, nodePropertyLoader );
    }

    @Override
    public PrimitiveLongIterator relationshipGetPropertyKeys( long relationshipId )
            throws EntityNotFoundException
    {
        return new PropertyKeyIdIterator( relationshipGetAllProperties( relationshipId ) );
    }

    @Override
    public Property relationshipGetProperty( long relationshipId, int propertyKeyId )
            throws EntityNotFoundException
    {
        return persistenceCache.relationshipGetProperty( relationshipId, propertyKeyId,
                                                         relationshipPropertyLoader );
    }

    @Override
    public Iterator<DefinedProperty> relationshipGetAllProperties( long nodeId )
            throws EntityNotFoundException
    {
        return persistenceCache.relationshipGetProperties( nodeId, relationshipPropertyLoader );
    }

    @Override
    public PrimitiveLongIterator graphGetPropertyKeys( KernelStatement state )
    {
        return persistenceCache.graphGetPropertyKeys( graphPropertyLoader );
    }

    @Override
    public Property graphGetProperty( int propertyKeyId )
    {
        return persistenceCache.graphGetProperty( graphPropertyLoader, propertyKeyId );
    }

    @Override
    public Iterator<DefinedProperty> graphGetAllProperties()
    {
        return persistenceCache.graphGetProperties( graphPropertyLoader );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( int labelId, int propertyKeyId )
    {
        return schemaCache.constraintsForLabelAndProperty( labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabel( int labelId )
    {
        return schemaCache.constraintsForLabel( labelId );
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetAll()
    {
        return schemaCache.constraints();
    }

    @Override
    public PrimitiveLongResourceIterator nodeGetUniqueFromIndexLookup(
            KernelStatement state,
            IndexDescriptor index,
            Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        return diskLayer.nodeGetUniqueFromIndexLookup( state, schemaCache.indexId( index ), value );
    }

    @Override
    public PrimitiveLongIterator nodesGetForLabel( KernelStatement state, int labelId )
    {
        return diskLayer.nodesGetForLabel( state, labelId );
    }

    @Override
    public PrimitiveLongResourceIterator nodesGetFromIndexLookup( KernelStatement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        return diskLayer.nodesGetFromIndexLookup( state, schemaCache.indexId( index ), value );
    }

    @Override
    public IndexDescriptor indexesGetForLabelAndPropertyKey( int labelId, int propertyKey )
    {
        return schemaCache.indexDescriptor( labelId, propertyKey );
    }

    @Override
    public InternalIndexState indexGetState( IndexDescriptor descriptor )
            throws IndexNotFoundKernelException
    {
        return indexingService.getIndexProxy( schemaCache.indexId( descriptor ) ).getState();
    }

    @Override
    public double indexUniqueValuesPercentage( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return indexingService.indexUniqueValuesPercentage( schemaCache.indexId( descriptor ) );
    }

    @Override
    public String indexGetFailure( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return diskLayer.indexGetFailure( descriptor );
    }

    @Override
    public int labelGetForName( String labelName )
    {
        return diskLayer.labelGetForName( labelName );
    }

    @Override
    public String labelGetName( int labelId ) throws LabelNotFoundKernelException
    {
        return diskLayer.labelGetName( labelId );
    }

    @Override
    public int propertyKeyGetForName( String propertyKeyName )
    {
        return diskLayer.propertyKeyGetForName( propertyKeyName );
    }

    @Override
    public int propertyKeyGetOrCreateForName( String propertyKeyName )
    {
        return diskLayer.propertyKeyGetOrCreateForName( propertyKeyName );
    }

    @Override
    public String propertyKeyGetName( int propertyKeyId ) throws PropertyKeyIdNotFoundKernelException
    {
        return diskLayer.propertyKeyGetName( propertyKeyId );
    }

    @Override
    public Iterator<Token> propertyKeyGetAllTokens()
    {
        return diskLayer.propertyKeyGetAllTokens();
    }

    @Override
    public Iterator<Token> labelsGetAllTokens()
    {
        return diskLayer.labelsGetAllTokens();
    }

    @Override
    public int relationshipTypeGetForName( String relationshipTypeName )
    {
        return diskLayer.relationshipTypeGetForName( relationshipTypeName );
    }

    @Override
    public String relationshipTypeGetName( int relationshipTypeId ) throws RelationshipTypeIdNotFoundKernelException
    {
        return diskLayer.relationshipTypeGetName( relationshipTypeId );
    }

    @Override
    public int labelGetOrCreateForName( String labelName ) throws TooManyLabelsException
    {
        return diskLayer.labelGetOrCreateForName( labelName );
    }

    @Override
    public int relationshipTypeGetOrCreateForName( String relationshipTypeName )
    {
        return diskLayer.relationshipTypeGetOrCreateForName( relationshipTypeName );
    }

    @Override
    public PrimitiveLongIterator nodeListRelationships( long nodeId, Direction direction )
            throws EntityNotFoundException
    {
        return persistenceCache.nodeGetRelationships( nodeId, direction );
    }

    @Override
    public PrimitiveLongIterator nodeListRelationships( long nodeId, Direction direction,
                                                        int[] relTypes ) throws EntityNotFoundException
    {
        return persistenceCache.nodeGetRelationships( nodeId, direction, relTypes );
    }

    @Override
    public int nodeGetDegree( long nodeId, Direction direction )
            throws EntityNotFoundException
    {
        return persistenceCache.nodeGetDegree( nodeId, direction );
    }

    @Override
    public int nodeGetDegree( long nodeId, Direction direction, int relType )
            throws EntityNotFoundException
    {
        return persistenceCache.nodeGetDegree( nodeId, relType, direction );
    }

    @Override
    public boolean nodeVisitDegrees( long nodeId, DegreeVisitor visitor )
    {
        return persistenceCache.nodeVisitDegrees( nodeId, visitor );
    }

    @Override
    public PrimitiveIntIterator nodeGetRelationshipTypes( long nodeId )
            throws EntityNotFoundException
    {
        return persistenceCache.nodeGetRelationshipTypes( nodeId );
    }

    @Override
    public <EXCEPTION extends Exception> void relationshipVisit( long relationshipId,
            RelationshipVisitor<EXCEPTION> relationshipVisitor ) throws EntityNotFoundException, EXCEPTION
    {
        RelationshipImpl relationship = persistenceCache.getRelationship( relationshipId );
        relationshipVisitor.visit( relationshipId, relationship.getTypeId(), relationship.getStartNodeId(),
                relationship.getEndNodeId());
    }

    @Override
    public Cursor expand( Cursor inputCursor, NeoRegister.Node.In nodeId, Register.Object.In<int[]> types,
                          Register.Object.In<Direction> expandDirection, NeoRegister.Relationship.Out relId,
                          NeoRegister.RelType.Out relType, Register.Object.Out<Direction> direction,
                          NeoRegister.Node.Out startNodeId, NeoRegister.Node.Out neighborNodeId )
    {
        return new StoreExpandCursor( this, inputCursor, nodeId, types, expandDirection, relId, relType, direction, startNodeId, neighborNodeId );
    }

    @Override
    public long countsForNode( int labelId )
    {
        return diskLayer.countsForNode( labelId );
    }

    @Override
    public long countsForRelationship( int startLabelId, int typeId, int endLabelId )
    {
        return diskLayer.countsForRelationship( startLabelId, typeId, endLabelId );
    }

    @Override
    public long highestNodeIdInUse()
    {
        return diskLayer.highestNodeIdInUse();
    }

    @Override
    public PrimitiveLongIterator nodesGetAll()
    {
        return diskLayer.nodesGetAll();
    }

    @Override
    public PrimitiveLongIterator relationshipsGetAll()
    {
        return diskLayer.relationshipsGetAll();
    }

    @Override
    public boolean relationshipExists( long relationshipId )
    {
        return diskLayer.relationshipExists( relationshipId );
    }

    @Override
    public long reserveNode()
    {
        return diskLayer.reserveNode();
    }

    @Override
    public long reserveRelationship()
    {
        return diskLayer.reserveRelationship();
    }

    @Override
    public void releaseNode( long id )
    {
        diskLayer.releaseNode( id );
    }

    @Override
    public void releaseRelationship( long id )
    {
        diskLayer.releaseRelationship( id );
    }
}
