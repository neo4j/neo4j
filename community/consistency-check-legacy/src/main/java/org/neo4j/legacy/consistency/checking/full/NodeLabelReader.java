/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.legacy.consistency.checking.full;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.legacy.consistency.checking.CheckerEngine;
import org.neo4j.legacy.consistency.checking.LabelChainWalker;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.store.RecordAccess;
import org.neo4j.legacy.consistency.store.RecordReference;

public class NodeLabelReader
{
    public static <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport> Set<Long> getListOfLabels(
            NodeRecord nodeRecord, RecordAccess records, CheckerEngine<RECORD, REPORT> engine )
    {
        final Set<Long> labels = new HashSet<>();

        NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( nodeRecord );
        if ( nodeLabels instanceof DynamicNodeLabels )
        {

            DynamicNodeLabels dynamicNodeLabels = (DynamicNodeLabels) nodeLabels;
            long firstRecordId = dynamicNodeLabels.getFirstDynamicRecordId();
            RecordReference<DynamicRecord> firstRecordReference = records.nodeLabels( firstRecordId );
            engine.comparativeCheck( firstRecordReference,
                    new LabelChainWalker<>(
                            new LabelChainWalker.Validator<RECORD, REPORT>()
                            {
                                @Override
                                public void onRecordNotInUse( DynamicRecord dynamicRecord,
                                                              CheckerEngine<RECORD, REPORT> engine )
                                {
                                }

                                @Override
                                public void onRecordChainCycle( DynamicRecord record,
                                                                CheckerEngine<RECORD, REPORT> engine )
                                {
                                }

                                @Override
                                public void onWellFormedChain( long[] labelIds,
                                                               CheckerEngine<RECORD, REPORT> engine,
                                                               RecordAccess records )
                                {
                                    copyToSet( labelIds, labels );
                                }
                            } ) );
        }
        else
        {
            copyToSet( nodeLabels.get( null ), labels );
        }

        return labels;
    }

    private static void copyToSet( long[] array, Set<Long> set )
    {
        for ( long labelId : array )
        {
            set.add( labelId );
        }
    }
}
