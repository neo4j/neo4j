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
package org.neo4j.kernel.impl.store;

import org.opentest4j.TestAbortedException;

import java.util.Collection;
import java.util.Iterator;

import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.string.UTF8;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class LabelTokenStoreConsistentReadTest extends RecordStoreConsistentReadTest<LabelTokenRecord, LabelTokenStore>
{

    private static final int NAME_RECORD_ID = 2;
    private static final byte[] NAME_RECORD_DATA = UTF8.encode( "TheLabel" );

    @Override
    protected LabelTokenStore getStore( NeoStores neoStores )
    {
        return neoStores.getLabelTokenStore();
    }

    @Override
    protected LabelTokenStore initialiseStore( NeoStores neoStores )
    {
        LabelTokenStore store = getStore( neoStores );
        LabelTokenRecord record = createExistingRecord( false );
        DynamicRecord nameRecord = new DynamicRecord( NAME_RECORD_ID );
        record.getNameRecords().clear();
        nameRecord.setData( NAME_RECORD_DATA );
        nameRecord.setInUse( true );
        record.addNameRecord( nameRecord );
        store.updateRecord( record );
        return store;
    }

    @Override
    protected LabelTokenRecord createNullRecord( long id )
    {
        return new LabelTokenRecord( (int) id ).initialize( false, 0 );
    }

    @Override
    protected LabelTokenRecord createExistingRecord( boolean light )
    {
        LabelTokenRecord record = new LabelTokenRecord( ID );
        record.setNameId( NAME_RECORD_ID );
        record.setInUse( true );
        if ( !light )
        {
            DynamicRecord nameRecord = new DynamicRecord( NAME_RECORD_ID );
            nameRecord.setInUse( true );
            nameRecord.setData( NAME_RECORD_DATA );
            record.addNameRecord( nameRecord );
        }
        return record;
    }

    @Override
    protected LabelTokenRecord getLight( long id, LabelTokenStore store )
    {
        throw new TestAbortedException( "No light loading of LabelTokenRecords" );
    }

    @Override
    protected void assertRecordsEqual( LabelTokenRecord actualRecord, LabelTokenRecord expectedRecord )
    {
        assertNotNull( actualRecord, "actualRecord" );
        assertNotNull( expectedRecord, "expectedRecord" );
        assertThat( actualRecord.getNameId() ).as( "getNameId" ).isEqualTo( expectedRecord.getNameId() );
        assertThat( actualRecord.getId() ).as( "getId" ).isEqualTo( expectedRecord.getId() );
        assertThat( actualRecord.getId() ).as( "getLongId" ).isEqualTo( expectedRecord.getId() );
        assertThat( actualRecord.isLight() ).as( "isLight" ).isEqualTo( expectedRecord.isLight() );

        Collection<DynamicRecord> actualNameRecords = actualRecord.getNameRecords();
        Collection<DynamicRecord> expectedNameRecords = expectedRecord.getNameRecords();
        assertThat( actualNameRecords.size() ).as( "getNameRecords.size" ).isEqualTo( expectedNameRecords.size() );
        Iterator<DynamicRecord> actualNRs = actualNameRecords.iterator();
        Iterator<DynamicRecord> expectedNRs = expectedNameRecords.iterator();
        int i = 0;
        while ( actualNRs.hasNext() && expectedNRs.hasNext() )
        {
            DynamicRecord actualNameRecord = actualNRs.next();
            DynamicRecord expectedNameRecord = expectedNRs.next();

            assertThat( actualNameRecord.getData() ).as( "[" + i + "]getData" ).isEqualTo( expectedNameRecord.getData() );
            assertThat( actualNameRecord.getLength() ).as( "[" + i + "]getLength" ).isEqualTo( expectedNameRecord.getLength() );
            assertThat( actualNameRecord.getNextBlock() ).as( "[" + i + "]getNextBlock" ).isEqualTo( expectedNameRecord.getNextBlock() );
            assertThat( actualNameRecord.getType() ).as( "[" + i + "]getType" ).isEqualTo( expectedNameRecord.getType() );
            assertThat( actualNameRecord.getId() ).as( "[" + i + "]getId" ).isEqualTo( expectedNameRecord.getId() );
            assertThat( actualNameRecord.getId() ).as( "[" + i + "]getLongId" ).isEqualTo( expectedNameRecord.getId() );
            assertThat( actualNameRecord.isStartRecord() ).as( "[" + i + "]isStartRecord" ).isEqualTo( expectedNameRecord.isStartRecord() );
            assertThat( actualNameRecord.inUse() ).as( "[" + i + "]inUse" ).isEqualTo( expectedNameRecord.inUse() );
            i++;
        }
    }
}
