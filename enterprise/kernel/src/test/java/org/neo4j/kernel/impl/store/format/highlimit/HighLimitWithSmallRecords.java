/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.kernel.impl.store.format.InternalRecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

/**
 * A {@link HighLimit} record format that forces records to be split in two units more often than the original format.
 */
public class HighLimitWithSmallRecords extends HighLimit
{
    public static final String NAME = "high_limit_with_small_records";
    public static final RecordFormats RECORD_FORMATS = new HighLimitWithSmallRecords();

    /**
     * We've added an extra byte to the header {@link BaseHighLimitRecordFormat}
     * This causes problems with splitting across values in this small records high limits format used in tests.
     * The +1 to the following 3 record sizes will cause it to not split across a value.
     */
    private static final int NODE_RECORD_SIZE = (NodeRecordFormat.RECORD_SIZE / 2) + 1;
    private static final int RELATIONSHIP_RECORD_SIZE = (RelationshipRecordFormat.RECORD_SIZE / 2) + 1;
    private static final int RELATIONSHIP_GROUP_RECORD_SIZE = (RelationshipGroupRecordFormat.RECORD_SIZE / 2) + 1;

    private HighLimitWithSmallRecords()
    {
    }

    public static void enable()
    {
        FeatureToggles.set( InternalRecordFormatSelector.class, InternalRecordFormatSelector.FORMAT_TOGGLE_NAME, NAME );
        RecordFormats formats = InternalRecordFormatSelector.select();
        if ( !HighLimitWithSmallRecords.class.equals( formats.getClass() ) )
        {
            throw new AssertionError( "Failed to enable " + NAME + " format via feature toggle. Selected: " + formats );
        }
    }

    public static boolean isEnabled()
    {
        String toggleValue = FeatureToggles.getString( InternalRecordFormatSelector.class,
                InternalRecordFormatSelector.FORMAT_TOGGLE_NAME, "" );
        return NAME.equals( toggleValue );
    }

    public static void disable()
    {
        FeatureToggles.clear( InternalRecordFormatSelector.class, InternalRecordFormatSelector.FORMAT_TOGGLE_NAME );
    }

    @Override
    public RecordFormat<NodeRecord> node()
    {
        return new NodeRecordFormat( NODE_RECORD_SIZE );
    }

    @Override
    public RecordFormat<RelationshipRecord> relationship()
    {
        return new RelationshipRecordFormat( RELATIONSHIP_RECORD_SIZE );
    }

    @Override
    public RecordFormat<RelationshipGroupRecord> relationshipGroup()
    {
        return new RelationshipGroupRecordFormat( RELATIONSHIP_GROUP_RECORD_SIZE );
    }
}
