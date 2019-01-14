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
package org.neo4j.consistency.checking;

import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.util.Iterator;
import java.util.function.Function;

import org.neo4j.consistency.checking.full.MandatoryProperties;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;

public class PropertyChain<RECORD extends PrimitiveRecord, REPORT extends ConsistencyReport.PrimitiveConsistencyReport>
        implements RecordField<RECORD,REPORT>, ComparativeRecordChecker<RECORD, PropertyRecord, REPORT>
{
    private final Function<RECORD,MandatoryProperties.Check<RECORD,REPORT>> mandatoryProperties;

    public PropertyChain( Function<RECORD,MandatoryProperties.Check<RECORD,REPORT>> mandatoryProperties )
    {
        this.mandatoryProperties = mandatoryProperties;
    }

    @Override
    public void checkConsistency( RECORD record, CheckerEngine<RECORD, REPORT> engine,
                                  RecordAccess records )
    {
        if ( !Record.NO_NEXT_PROPERTY.is( record.getNextProp() ) )
        {
            // Check the whole chain here instead of scattered during multiple checks.
            // This type of check obviously favors chains with good locality, performance-wise.
            Iterator<PropertyRecord> props = records.rawPropertyChain( record.getNextProp() );
            PropertyRecord firstProp = props.next();
            if ( !Record.NO_PREVIOUS_PROPERTY.is( firstProp.getPrevProp() ) )
            {
                engine.report().propertyNotFirstInChain( firstProp );
            }

            final MutableIntSet keys = new IntHashSet();
            final MutableLongSet propertyRecordIds = new LongHashSet( 8 );
            propertyRecordIds.add( firstProp.getId() );
            try ( MandatoryProperties.Check<RECORD,REPORT> mandatory = mandatoryProperties.apply( record ) )
            {
                checkChainItem( firstProp, engine, keys, mandatory );

                // Check the whole chain here. We also take the opportunity to check mandatory property constraints.
                PropertyRecord prop = firstProp;
                while ( props.hasNext() )
                {
                    PropertyRecord nextProp = props.next();
                    if ( !propertyRecordIds.add( nextProp.getId() ) )
                    {
                        engine.report().propertyChainContainsCircularReference( prop );
                        break;
                    }
                    checkChainItem( nextProp, engine, keys, mandatory );
                    prop = nextProp;
                }
            }
        }
    }

    private void checkChainItem( PropertyRecord property, CheckerEngine<RECORD,REPORT> engine,
            MutableIntSet keys, MandatoryProperties.Check<RECORD,REPORT> mandatory )
    {
        if ( !property.inUse() )
        {
            engine.report().propertyNotInUse( property );
        }
        else
        {
            int[] keysInRecord = ChainCheck.keys( property );
            if ( mandatory != null )
            {
                mandatory.receive( keysInRecord );
            }
            for ( int key : keysInRecord )
            {
                if ( !keys.add( key ) )
                {
                    engine.report().propertyKeyNotUniqueInChain();
                }
            }
        }
    }

    @Override
    public long valueFrom( RECORD record )
    {
        return record.getNextProp();
    }

    @Override
    public void checkReference( RECORD record, PropertyRecord property, CheckerEngine<RECORD, REPORT> engine,
                                RecordAccess records )
    {
        if ( !property.inUse() )
        {
            engine.report().propertyNotInUse( property );
        }
        else
        {
            if ( !Record.NO_PREVIOUS_PROPERTY.is( property.getPrevProp() ) )
            {
                engine.report().propertyNotFirstInChain( property );
            }
            new ChainCheck<RECORD, REPORT>().checkReference( record, property, engine, records );
        }
    }
}
