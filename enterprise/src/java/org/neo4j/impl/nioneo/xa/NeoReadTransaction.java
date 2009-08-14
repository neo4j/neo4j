/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.nioneo.xa;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.impl.nioneo.store.NeoStore;
import org.neo4j.impl.nioneo.store.NodeRecord;
import org.neo4j.impl.nioneo.store.NodeStore;
import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.nioneo.store.PropertyIndexData;
import org.neo4j.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.impl.nioneo.store.PropertyIndexStore;
import org.neo4j.impl.nioneo.store.PropertyRecord;
import org.neo4j.impl.nioneo.store.PropertyStore;
import org.neo4j.impl.nioneo.store.PropertyType;
import org.neo4j.impl.nioneo.store.Record;
import org.neo4j.impl.nioneo.store.RelationshipData;
import org.neo4j.impl.nioneo.store.RelationshipRecord;
import org.neo4j.impl.nioneo.store.RelationshipStore;
import org.neo4j.impl.nioneo.store.StoreFailureException;
import org.neo4j.impl.util.ArrayMap;

class NeoReadTransaction
{
    private final NeoStore neoStore;
    
    public NeoReadTransaction( NeoStore neoStore )
    {
        this.neoStore = neoStore;
    }

    private NodeStore getNodeStore()
    {
        return neoStore.getNodeStore();
    }

    private RelationshipStore getRelationshipStore()
    {
        return neoStore.getRelationshipStore();
    }

    private PropertyStore getPropertyStore()
    {
        return neoStore.getPropertyStore();
    }

    public boolean nodeLoadLight( int nodeId )
    {
        return getNodeStore().loadLightNode( nodeId );
    }

    public RelationshipData relationshipLoad( int id )
    {
        RelationshipRecord relRecord = getRelationshipStore().getLightRel( id );
        if ( relRecord != null )
        {
            return new RelationshipData( id, relRecord.getFirstNode(),
                relRecord.getSecondNode(), relRecord.getType() );
        }
        return null;
    }

    public Iterable<RelationshipData> nodeGetRelationships( int nodeId )
    {
        NodeRecord nodeRecord = getNodeStore().getRecord( nodeId );
        int nextRel = nodeRecord.getNextRel();
        List<RelationshipData> rels = new ArrayList<RelationshipData>();
        while ( nextRel != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipRecord relRecord = 
                getRelationshipStore().getRecord( nextRel );
            int firstNode = relRecord.getFirstNode();
            int secondNode = relRecord.getSecondNode();
            rels.add( new RelationshipData( nextRel, firstNode, secondNode,
                relRecord.getType() ) );
            if ( firstNode == nodeId )
            {
                nextRel = relRecord.getFirstNextRel();
            }
            else if ( secondNode == nodeId )
            {
                nextRel = relRecord.getSecondNextRel();
            }
            else
            {
                throw new RuntimeException( "GAH" );
            }
        }
        return rels;
    }

    public ArrayMap<Integer,PropertyData> relGetProperties( int relId )
    {
        RelationshipRecord relRecord = getRelationshipStore().getRecord( relId );
        if ( !relRecord.inUse() )
        {
            throw new StoreFailureException( "Relationship[" + relId + 
                "] not in use" );
        }
        int nextProp = relRecord.getNextProp();
        ArrayMap<Integer,PropertyData> propertyMap = 
            new ArrayMap<Integer,PropertyData>( 9, false, true );
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord propRecord = 
                getPropertyStore().getLightRecord( nextProp );
            propertyMap.put( propRecord.getKeyIndexId(), 
                new PropertyData( propRecord.getId(),                      
                    propertyGetValueOrNull( propRecord ) ) );
            nextProp = propRecord.getNextProp();
        }
        return propertyMap;
    }

    ArrayMap<Integer,PropertyData> nodeGetProperties( int nodeId )
    {
        NodeRecord nodeRecord = getNodeStore().getRecord( nodeId );
            
        int nextProp = nodeRecord.getNextProp();
        ArrayMap<Integer,PropertyData> propertyMap = 
            new ArrayMap<Integer,PropertyData>( 9, false, true );
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord propRecord = getPropertyStore().getLightRecord( nextProp );
            propertyMap.put( propRecord.getKeyIndexId(), 
                new PropertyData( propRecord.getId(), 
                    propertyGetValueOrNull( propRecord ) ) );
            nextProp = propRecord.getNextProp();
        }
        return propertyMap;
    }
    
    public Object propertyGetValueOrNull( PropertyRecord propertyRecord )
    {
        PropertyType type = propertyRecord.getType();
        if ( type == PropertyType.INT )
        {
            return (int) propertyRecord.getPropBlock();
        }
        if ( type == PropertyType.STRING )
        {
            return null;
        }
        if ( type == PropertyType.BOOL )
        {
            if ( propertyRecord.getPropBlock() == 1 )
            {
                return Boolean.valueOf( true );
            }
            return Boolean.valueOf( false );
        }
        if ( type == PropertyType.DOUBLE )
        {
            return new Double( Double.longBitsToDouble( 
                propertyRecord.getPropBlock() ) );
        }
        if ( type == PropertyType.FLOAT )
        {
            return new Float( Float.intBitsToFloat( 
                (int) propertyRecord.getPropBlock() ) );
        }
        if ( type == PropertyType.LONG )
        {
            return propertyRecord.getPropBlock();
        }
        if ( type == PropertyType.BYTE )
        {
            return (byte) propertyRecord.getPropBlock();
        }
        if ( type == PropertyType.CHAR )
        {
            return (char) propertyRecord.getPropBlock();
        }
        if ( type == PropertyType.ARRAY )
        {
            return null;
        }
        if ( type == PropertyType.SHORT )
        {
            return (short) propertyRecord.getPropBlock();
        }
        throw new RuntimeException( "Unknown type: " + type );
    }

    public Object propertyGetValue( int id )
    {
        PropertyRecord propertyRecord = getPropertyStore().getRecord( id );
        if ( propertyRecord.isLight() )
        {
            getPropertyStore().makeHeavy( propertyRecord );
        }
        PropertyType type = propertyRecord.getType();
        if ( type == PropertyType.INT )
        {
            return (int) propertyRecord.getPropBlock();
        }
        if ( type == PropertyType.STRING )
        {
            return getPropertyStore().getStringFor( propertyRecord );
        }
        if ( type == PropertyType.BOOL )
        {
            if ( propertyRecord.getPropBlock() == 1 )
            {
                return Boolean.valueOf( true );
            }
            return Boolean.valueOf( false );
        }
        if ( type == PropertyType.DOUBLE )
        {
            return new Double( Double.longBitsToDouble( 
                propertyRecord.getPropBlock() ) );
        }
        if ( type == PropertyType.FLOAT )
        {
            return new Float( Float.intBitsToFloat( 
                (int) propertyRecord.getPropBlock() ) );
        }
        if ( type == PropertyType.LONG )
        {
            return propertyRecord.getPropBlock();
        }
        if ( type == PropertyType.BYTE )
        {
            return (byte) propertyRecord.getPropBlock();
        }
        if ( type == PropertyType.CHAR )
        {
            return (char) propertyRecord.getPropBlock();
        }
        if ( type == PropertyType.ARRAY )
        {
            return getPropertyStore().getArrayFor( propertyRecord );
        }
        if ( type == PropertyType.SHORT )
        {
            return (short) propertyRecord.getPropBlock();
        }
        throw new RuntimeException( "Unknown type: " + type );
    }

    String getPropertyIndex( int id )
    {
        PropertyIndexStore indexStore = getPropertyStore().getIndexStore();
        PropertyIndexRecord index = indexStore.getRecord( id );
        if ( index.isLight() )
        {
            indexStore.makeHeavy( index );
        }
        return indexStore.getStringFor( index );
    }

    PropertyIndexData[] getPropertyIndexes( int count )
    {
        PropertyIndexStore indexStore = getPropertyStore().getIndexStore();
        return indexStore.getPropertyIndexes( count );
    }
}
