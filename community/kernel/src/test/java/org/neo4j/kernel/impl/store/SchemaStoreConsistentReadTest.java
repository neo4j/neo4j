/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.kernel.impl.store.record.DynamicRecord;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;

// This one might be good enough to cover all AbstractDynamicStore subclasses,
// including DynamicArrayStore and DynamicStringStore.
class SchemaStoreConsistentReadTest extends RecordStoreConsistentReadTest<DynamicRecord, SchemaStore>
{
    private static final byte[] EXISTING_RECORD_DATA = "Random bytes".getBytes();

    @Override
    protected SchemaStore getStore( NeoStores neoStores )
    {
        return neoStores.getSchemaStore();
    }

    @Override
    protected DynamicRecord createNullRecord( long id )
    {
        DynamicRecord record = new DynamicRecord( id );
        record.setNextBlock( 0 );
        record.setData( new byte[0] );
        return record;
    }

    @Override
    protected DynamicRecord createExistingRecord( boolean light )
    {
        DynamicRecord record = new DynamicRecord( ID );
        record.setInUse( true );
        record.setStartRecord( true );
        record.setLength( EXISTING_RECORD_DATA.length );
        record.setData( EXISTING_RECORD_DATA );
        return record;
    }

    @Override
    protected DynamicRecord getLight( long id, SchemaStore store )
    {
        throw new TestAbortedException( "Light loading of DynamicRecords is a little different" );
    }

    @Override
    protected void assertRecordsEqual( DynamicRecord actualRecord, DynamicRecord expectedRecord )
    {
        assertNotNull( actualRecord, "actualRecord" );
        assertNotNull( expectedRecord, "expectedRecord" );
        assertThat( "getData", actualRecord.getData(), is( expectedRecord.getData() ) );
        assertThat( "getLength", actualRecord.getLength(), is( expectedRecord.getLength() ) );
        assertThat( "getNextBlock", actualRecord.getNextBlock(), is( expectedRecord.getNextBlock() ) );
        assertThat( "getType", actualRecord.getType(), is( expectedRecord.getType() ) );
        assertThat( "getId", actualRecord.getId(), is( expectedRecord.getId() ) );
        assertThat( "getLongId", actualRecord.getId(), is( expectedRecord.getId() ) );
        assertThat( "isStartRecord", actualRecord.isStartRecord(), is( expectedRecord.isStartRecord() ) );
    }
}
