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
package org.neo4j.kernel.impl.newapi;

import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.FILTER;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.FILTER_TX_STATE;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.GROUP;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.NO_INCOMING_OF_TYPE;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.NO_LOOP_OF_TYPE;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.NO_OUTGOING_OF_TYPE;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.parseEncoding;
import static org.neo4j.kernel.impl.newapi.References.clearEncoding;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

public class ReferencesTest
{
    // This value the largest possible high limit id +1 (see HighLimitV3_1_0)
    private static long MAX_ID_LIMIT = 1L << 50;

    @Test
    public void shouldPreserveNoId()
    {
        assertThat( RelationshipReferenceEncoding.encodeForFiltering( NO_ID ), equalTo( (long) NO_ID ) );
        assertThat( RelationshipReferenceEncoding.encodeForTxStateFiltering( NO_ID ), equalTo( (long) NO_ID ) );
        assertThat( RelationshipReferenceEncoding.encodeGroup( NO_ID ), equalTo( (long) NO_ID ) );
        assertThat( RelationshipReferenceEncoding.encodeNoIncomingRels( NO_ID ), equalTo( (long) NO_ID ) );
        assertThat( RelationshipReferenceEncoding.encodeNoOutgoingRels( NO_ID ), equalTo( (long) NO_ID ) );
        assertThat( RelationshipReferenceEncoding.encodeNoLoopRels( NO_ID ), equalTo( (long) NO_ID ) );

        assertThat( GroupReferenceEncoding.encodeRelationship( NO_ID ), equalTo( (long) NO_ID ) );
    }

    @Test
    public void shouldClearFlags()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            int token = random.nextInt(Integer.MAX_VALUE);

            assertThat( clearEncoding( RelationshipReferenceEncoding.encodeGroup( reference ) ), equalTo( reference ) );
            assertThat( clearEncoding( RelationshipReferenceEncoding.encodeForFiltering( reference ) ), equalTo( reference ) );
            assertThat( clearEncoding( RelationshipReferenceEncoding.encodeForTxStateFiltering( reference ) ), equalTo( reference ) );
            assertThat( clearEncoding( RelationshipReferenceEncoding.encodeNoIncomingRels( token ) ), equalTo( (long) token ) );
            assertThat( clearEncoding( RelationshipReferenceEncoding.encodeNoOutgoingRels( token ) ), equalTo( (long) token ) );
            assertThat( clearEncoding( RelationshipReferenceEncoding.encodeNoLoopRels( token ) ), equalTo( (long) token ) );

            assertThat( clearEncoding( GroupReferenceEncoding.encodeRelationship( reference ) ), equalTo( reference ) );
        }
    }

    // Relationship

    @Test
    public void encodeForFiltering()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertNotEquals( FILTER, parseEncoding( reference ) );
            assertEquals( FILTER, parseEncoding( RelationshipReferenceEncoding.encodeForFiltering( reference ) ) );
            assertTrue( "encoded reference is negative", RelationshipReferenceEncoding.encodeForFiltering( reference ) < 0 );
        }
    }

    @Test
    public void encodeForTxStateFiltering()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertNotEquals( FILTER_TX_STATE, parseEncoding( reference ) );
            assertEquals( FILTER_TX_STATE, parseEncoding( RelationshipReferenceEncoding.encodeForTxStateFiltering( reference ) ) );
            assertTrue( "encoded reference is negative", RelationshipReferenceEncoding.encodeForTxStateFiltering( reference ) < 0 );
        }
    }

    @Test
    public void encodeFromGroup()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertNotEquals( GROUP, parseEncoding( reference ) );
            assertEquals( GROUP, parseEncoding( RelationshipReferenceEncoding.encodeGroup( reference ) ) );
            assertTrue( "encoded reference is negative", RelationshipReferenceEncoding.encodeGroup( reference ) < 0 );
        }
    }

    @Test
    public void encodeNoIncomingRels()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            int token = random.nextInt(Integer.MAX_VALUE);
            assertNotEquals( NO_INCOMING_OF_TYPE, parseEncoding( token ) );
            assertEquals( NO_INCOMING_OF_TYPE, parseEncoding( RelationshipReferenceEncoding.encodeNoIncomingRels( token ) ) );
            assertTrue( "encoded reference is negative", RelationshipReferenceEncoding.encodeNoIncomingRels( token ) < 0 );
        }
    }

    @Test
    public void encodeNoOutgoingRels()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            int token = random.nextInt(Integer.MAX_VALUE);
            assertNotEquals( NO_OUTGOING_OF_TYPE, parseEncoding( token ) );
            assertEquals( NO_OUTGOING_OF_TYPE, parseEncoding( RelationshipReferenceEncoding.encodeNoOutgoingRels( token ) ) );
            assertTrue( "encoded reference is negative", RelationshipReferenceEncoding.encodeNoOutgoingRels( token ) < 0 );
        }
    }

    @Test
    public void encodeNoLoopRels()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            int token = random.nextInt(Integer.MAX_VALUE);
            assertNotEquals( NO_LOOP_OF_TYPE, parseEncoding( token ) );
            assertEquals( NO_LOOP_OF_TYPE, parseEncoding( RelationshipReferenceEncoding.encodeNoLoopRels( token ) ) );
            assertTrue( "encoded reference is negative", RelationshipReferenceEncoding.encodeNoLoopRels( token ) < 0 );
        }
    }

    // Group

    @Test
    public void encodeRelationship()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertFalse( GroupReferenceEncoding.isRelationship( reference ) );
            assertTrue( GroupReferenceEncoding.isRelationship( GroupReferenceEncoding.encodeRelationship( reference ) ) );
            assertTrue( "encoded reference is negative", GroupReferenceEncoding.encodeRelationship( reference ) < 0 );
        }
    }
}
