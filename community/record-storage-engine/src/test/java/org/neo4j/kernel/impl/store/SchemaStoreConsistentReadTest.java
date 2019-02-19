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

import org.neo4j.kernel.impl.store.record.SchemaRecord;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SchemaStoreConsistentReadTest extends RecordStoreConsistentReadTest<SchemaRecord, SchemaStore>
{
    @Override
    protected SchemaStore getStore( NeoStores neoStores )
    {
        return neoStores.getSchemaStore();
    }

    @Override
    protected SchemaRecord createNullRecord( long id )
    {
        return new SchemaRecord( id ); // This is what it looks like when an unused record is force-read from the store.
    }

    @Override
    protected SchemaRecord createExistingRecord( boolean light )
    {
        SchemaRecord record = new SchemaRecord( ID );
        record.initialize( true, 42 );
        return record;
    }

    @Override
    protected SchemaRecord getLight( long id, SchemaStore store )
    {
        return getHeavy( store, id );
    }

    @Override
    protected void assertRecordsEqual( SchemaRecord actualRecord, SchemaRecord expectedRecord )
    {
        assertNotNull( actualRecord, "actualRecord" );
        assertNotNull( expectedRecord, "expectedRecord" );
        assertThat( "isConstraint", actualRecord.isConstraint(), is( expectedRecord.isConstraint() ) );
        assertThat( "getNextProp", actualRecord.getNextProp(), is( expectedRecord.getNextProp() ) );
        assertThat( "getId", actualRecord.getId(), is( expectedRecord.getId() ) );
        assertThat( "getLongId", actualRecord.getId(), is( expectedRecord.getId() ) );
    }
}
