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
package org.neo4j.consistency.checking;

import java.util.Arrays;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;

public class ChainCheck<RECORD extends PrimitiveRecord, REPORT extends ConsistencyReport.PrimitiveConsistencyReport>
        implements ComparativeRecordChecker<RECORD, PropertyRecord, REPORT>
{
    private static final int MAX_BLOCK_PER_RECORD_COUNT = 4;
    private final PrimitiveIntSet keys = Primitive.intSet();

    @Override
    public void checkReference( RECORD record, PropertyRecord property, CheckerEngine<RECORD, REPORT> engine,
                                RecordAccess records )
    {
        for ( int key : keys( property ) )
        {
            if ( !keys.add( key ) )
            {
                engine.report().propertyKeyNotUniqueInChain();
            }
        }
        if ( !Record.NO_NEXT_PROPERTY.is( property.getNextProp() ) )
        {
            engine.comparativeCheck( records.property( property.getNextProp() ), this );
        }
    }

    public static int[] keys( PropertyRecord property )
    {
        int[] toStartWith = new int[ MAX_BLOCK_PER_RECORD_COUNT ];
        int index = 0;
        for ( PropertyBlock propertyBlock : property )
        {
            toStartWith[index++] = propertyBlock.getKeyIndexId();
        }
        return Arrays.copyOf( toStartWith, index );
    }
}
