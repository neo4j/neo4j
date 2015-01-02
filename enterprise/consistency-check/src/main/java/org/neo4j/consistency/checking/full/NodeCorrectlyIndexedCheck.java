/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.consistency.checking.full;

import java.util.List;
import java.util.Set;

import org.neo4j.consistency.checking.CheckerEngine;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;

public class NodeCorrectlyIndexedCheck implements RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport>
{
    private final IndexAccessors indexes;
    private final PropertyReader propertyReader;

    public NodeCorrectlyIndexedCheck( IndexAccessors indexes,
                                      PropertyReader propertyReader )
    {
        this.indexes = indexes;
        this.propertyReader = propertyReader;
    }

    @Override
    public void check( NodeRecord record,
                       CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine,
                       RecordAccess records )
    {
        Set<Long> labels = NodeLabelReader.getListOfLabels( record, records, engine );
        for ( IndexRule indexRule : indexes.rules() )
        {
            if ( !labels.contains( (long) indexRule.getLabel() ) )
            {
                continue;
            }

            List<PropertyBlock> properties = propertyReader.propertyBlocks( record );
            PropertyBlock property = propertyWithKey( properties, indexRule.getPropertyKey() );

            if ( property == null )
            {
                continue;
            }

            try ( IndexReader reader = indexes.accessorFor( indexRule ).newReader() )
            {
                Object propertyValue = propertyReader.propertyValue( property ).value();
                long nodeId = record.getId();

                if ( indexRule.isConstraintIndex() )
                {
                    verifyNodeCorrectlyIndexedUniquely( nodeId, propertyValue, engine, indexRule, reader );
                }
                else
                {
                    verifyNodeCorrectlyIndexed( nodeId, propertyValue, engine, indexRule, reader );
                }
            }
        }
    }

    private void verifyNodeCorrectlyIndexedUniquely(
            long nodeId,
            Object propertyValue,
            CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine,
            IndexRule indexRule,
            IndexReader reader )
    {
        PrimitiveLongIterator indexedNodeIds = reader.lookup( propertyValue );
        boolean found = false;

        while ( indexedNodeIds.hasNext() )
        {
            long indexedNodeId = indexedNodeIds.next();

            if ( nodeId == indexedNodeId )
            {
                found = true;
            }
            else
            {
                engine.report().uniqueIndexNotUnique( indexRule, propertyValue, indexedNodeId );
            }
        }

        if ( !found )
        {
            engine.report().notIndexed( indexRule, propertyValue );
        }
    }

    private void verifyNodeCorrectlyIndexed(
            long nodeId,
            Object propertyValue,
            CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine,
            IndexRule indexRule,
            IndexReader reader )
    {
        if ( !reader.hasIndexed( nodeId, propertyValue ) )
        {
            engine.report().notIndexed( indexRule, propertyValue );
        }
    }

    private PropertyBlock propertyWithKey( List<PropertyBlock> propertyBlocks, int propertyKey )
    {
        for ( PropertyBlock propertyBlock : propertyBlocks )
        {
            if ( propertyBlock.getKeyIndexId() == propertyKey )
            {
                return propertyBlock;
            }
        }
        return null;
    }

    @Override
    public void checkChange( NodeRecord oldRecord, NodeRecord newRecord,
                             CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine,
                             DiffRecordAccess records )
    {
        check( newRecord, engine, records );
    }
}
