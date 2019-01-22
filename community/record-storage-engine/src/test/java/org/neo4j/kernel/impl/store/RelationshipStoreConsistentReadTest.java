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

import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

class RelationshipStoreConsistentReadTest extends RecordStoreConsistentReadTest<RelationshipRecord, RelationshipStore>
{
    // Constants for the contents of the existing record
    private static final int FIRST_NODE = 2;
    private static final int SECOND_NODE = 3;
    private static final int TYPE = 4;
    private static final int FIRST_PREV_REL = 5;
    private static final int FIRST_NEXT_REL = 6;
    private static final int SECOND_PREV_REL = 7;
    private static final int SECOND_NEXT_REL = 8;

    @Override
    protected RelationshipRecord createNullRecord( long id )
    {
        RelationshipRecord record = new RelationshipRecord( id, false, 0, 0, 0, 0, 0, 0, 0, false, false );
        record.setNextProp( 0 );
        return record;
    }

    @Override
    protected RelationshipRecord createExistingRecord( boolean light )
    {
        return new RelationshipRecord(
                ID, true, FIRST_NODE, SECOND_NODE, TYPE, FIRST_PREV_REL,
                FIRST_NEXT_REL, SECOND_PREV_REL, SECOND_NEXT_REL, true, true );
    }

    @Override
    protected RelationshipRecord getLight( long id, RelationshipStore store )
    {
        return store.getRecord( id, store.newRecord(), NORMAL );
    }

    @Override
    protected void assertRecordsEqual( RelationshipRecord actualRecord, RelationshipRecord expectedRecord )
    {
        assertNotNull( actualRecord, "actualRecord" );
        assertNotNull( expectedRecord, "expectedRecord" );
        assertThat( "getFirstNextRel", actualRecord.getFirstNextRel(), is( expectedRecord.getFirstNextRel() ) );
        assertThat( "getFirstNode", actualRecord.getFirstNode(), is( expectedRecord.getFirstNode() ) );
        assertThat( "getFirstPrevRel", actualRecord.getFirstPrevRel(), is( expectedRecord.getFirstPrevRel() ) );
        assertThat( "getSecondNextRel", actualRecord.getSecondNextRel(), is( expectedRecord.getSecondNextRel() ) );
        assertThat( "getSecondNode", actualRecord.getSecondNode(), is( expectedRecord.getSecondNode() ) );
        assertThat( "getSecondPrevRel", actualRecord.getSecondPrevRel(), is( expectedRecord.getSecondPrevRel() ) );
        assertThat( "getType", actualRecord.getType(), is( expectedRecord.getType() ) );
        assertThat( "isFirstInFirstChain", actualRecord.isFirstInFirstChain(), is( expectedRecord.isFirstInFirstChain() ) );
        assertThat( "isFirstInSecondChain", actualRecord.isFirstInSecondChain(), is( expectedRecord.isFirstInSecondChain() ) );
        assertThat( "getId", actualRecord.getId(), is( expectedRecord.getId() ) );
        assertThat( "getLongId", actualRecord.getId(), is( expectedRecord.getId() ) );
        assertThat( "getNextProp", actualRecord.getNextProp(), is( expectedRecord.getNextProp() ) );
        assertThat( "inUse", actualRecord.inUse(), is( expectedRecord.inUse() ) );
    }

    @Override
    protected RelationshipStore getStore( NeoStores neoStores )
    {
        return neoStores.getRelationshipStore();
    }
}
