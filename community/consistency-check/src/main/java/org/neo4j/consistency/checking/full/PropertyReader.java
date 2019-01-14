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
package org.neo4j.consistency.checking.full;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

class PropertyReader implements PropertyAccessor
{
    private final PropertyStore propertyStore;
    private final NodeStore nodeStore;

    PropertyReader( StoreAccess storeAccess )
    {
        this.propertyStore = storeAccess.getRawNeoStores().getPropertyStore();
        this.nodeStore = storeAccess.getRawNeoStores().getNodeStore();
    }

    Collection<PropertyRecord> getPropertyRecordChain( long firstPropertyRecordId ) throws CircularPropertyRecordChainException
    {
        List<PropertyRecord> records = new ArrayList<>();
        visitPropertyRecordChain( firstPropertyRecordId, record ->
        {
            records.add( record );
            return false; // please continue
        } );
        return records;
    }

    List<PropertyBlock> propertyBlocks( Collection<PropertyRecord> records )
    {
        List<PropertyBlock> propertyBlocks = new ArrayList<>();
        for ( PropertyRecord record : records )
        {
            for ( PropertyBlock block : record )
            {
                propertyBlocks.add( block );
            }
        }
        return propertyBlocks;
    }

    private boolean visitPropertyRecordChain( long firstPropertyRecordId, Visitor<PropertyRecord,RuntimeException> visitor )
            throws CircularPropertyRecordChainException
    {
        if ( Record.NO_NEXT_PROPERTY.is( firstPropertyRecordId ) )
        {
            return false;
        }

        PrimitiveLongSet visitedPropertyRecordIds = Primitive.longSet( 8 );
        visitedPropertyRecordIds.add( firstPropertyRecordId );
        long nextProp = firstPropertyRecordId;
        while ( !Record.NO_NEXT_PROPERTY.is( nextProp ) )
        {
            PropertyRecord propRecord = propertyStore.getRecord( nextProp, propertyStore.newRecord(), FORCE );
            nextProp = propRecord.getNextProp();
            if ( !Record.NO_NEXT_PROPERTY.is( nextProp ) && !visitedPropertyRecordIds.add( nextProp ) )
            {
                throw new CircularPropertyRecordChainException( propRecord );
            }
            if ( visitor.visit( propRecord ) )
            {
                return true;
            }
        }
        return false;
    }

    public Value propertyValue( PropertyBlock block )
    {
        return block.getType().value( block, propertyStore );
    }

    @Override
    public Value getPropertyValue( long nodeId, int propertyKeyId )
    {
        NodeRecord nodeRecord = nodeStore.newRecord();
        if ( nodeStore.getRecord( nodeId, nodeRecord, FORCE ).inUse() )
        {
            SpecificValueVisitor visitor = new SpecificValueVisitor( propertyKeyId );
            try
            {
                if ( visitPropertyRecordChain( nodeRecord.getNextProp(), visitor ) )
                {
                    return visitor.foundPropertyValue;
                }
            }
            catch ( CircularPropertyRecordChainException e )
            {
                // If we discover a circular reference and still haven't found the property then we won't find it.
                // There are other places where this circular reference will be logged as an inconsistency,
                // so simply catch this exception here and let this method return NO_VALUE below.
            }
        }
        return Values.NO_VALUE;
    }

    private class SpecificValueVisitor implements Visitor<PropertyRecord,RuntimeException>
    {
        private final int propertyKeyId;
        private Value foundPropertyValue;

        SpecificValueVisitor( int propertyKeyId )
        {
            this.propertyKeyId = propertyKeyId;
        }

        @Override
        public boolean visit( PropertyRecord element ) throws RuntimeException
        {
            for ( PropertyBlock block : element )
            {
                if ( block.getKeyIndexId() == propertyKeyId )
                {
                    foundPropertyValue = propertyValue( block );
                    return true;
                }
            }
            return false;
        }
    }

    static class CircularPropertyRecordChainException extends Exception
    {
        private final PropertyRecord propertyRecord;

        CircularPropertyRecordChainException( PropertyRecord propertyRecord )
        {
            this.propertyRecord = propertyRecord;
        }

        PropertyRecord propertyRecordClosingTheCircle()
        {
            return propertyRecord;
        }
    }
}
