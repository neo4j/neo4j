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

import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.map.primitive.IntObjectMap;

import java.util.Collection;
import java.util.List;

import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.consistency.checking.full.PropertyAndNodeIndexedCheck.entityIntersectsSchema;
import static org.neo4j.consistency.checking.full.PropertyAndNodeIndexedCheck.getPropertyValues;
import static org.neo4j.consistency.checking.full.PropertyAndNodeIndexedCheck.properties;

public class RelationshipToIndexCheck implements RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>
{
    private final IndexAccessors indexes;
    private final StoreIndexDescriptor[] relationshipIndexes;
    private final PropertyReader propertyReader;

    RelationshipToIndexCheck( List<StoreIndexDescriptor> relationshipIndexes, IndexAccessors indexes, PropertyReader propertyReader )
    {
        this.relationshipIndexes = relationshipIndexes.toArray( new StoreIndexDescriptor[0] );
        this.indexes = indexes;
        this.propertyReader = propertyReader;
    }

    @Override
    public void check( RelationshipRecord record, CheckerEngine<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport> engine,
            RecordAccess records )
    {
        try
        {
            IntObjectMap<PropertyBlock> propertyMap = null;
            for ( StoreIndexDescriptor index : relationshipIndexes )
            {
                SchemaDescriptor schema = index.schema();
                if ( ArrayUtils.contains( schema.getEntityTokenIds(), record.getType() ) )
                {
                    if ( propertyMap == null )
                    {
                        Collection<PropertyRecord> propertyRecs = propertyReader.getPropertyRecordChain( record.getNextProp() );
                        propertyMap = properties( propertyReader.propertyBlocks( propertyRecs ) );
                    }

                    if ( entityIntersectsSchema( propertyMap, schema ) )
                    {
                        Value[] values = getPropertyValues( propertyReader, propertyMap, schema.getPropertyIds() );
                        try ( IndexReader reader = indexes.accessorFor( index ).newReader() )
                        {
                            long entityId = record.getId();
                            long count = reader.countIndexedNodes( entityId, schema.getPropertyIds(), values );
                            reportIncorrectIndexCount( values, engine, index, count );
                        }
                    }
                }
            }
        }
        catch ( PropertyReader.CircularPropertyRecordChainException e )
        {
            // The property chain contains a circular reference and is therefore not sane.
            // Skip it, since this inconsistency has been reported by PropertyChain checker already.
        }
    }

    private void reportIncorrectIndexCount( Value[] values, CheckerEngine<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport> engine,
            StoreIndexDescriptor index, long count )
    {
        if ( count == 0 )
        {
            engine.report().notIndexed( index, Values.asObjects( values ) );
        }
        else if ( count != 1 )
        {
            engine.report().indexedMultipleTimes( index, Values.asObjects( values ), count );
        }
    }
}
