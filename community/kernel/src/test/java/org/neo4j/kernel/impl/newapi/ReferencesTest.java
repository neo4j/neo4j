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
package org.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.newapi.References.clearEncoding;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.FILTER;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.FILTER_TX_STATE;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.GROUP;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.NO_INCOMING_OF_TYPE;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.NO_LOOP_OF_TYPE;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.NO_OUTGOING_OF_TYPE;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.encodeGroup;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.parseEncoding;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

class ReferencesTest
{
    // This value the largest possible high limit id +1 (see HighLimitV3_1_0)
    private static long MAX_ID_LIMIT = 1L << 50;

    @Test
    void shouldPreserveNoId()
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
    void shouldClearFlags()
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
    void encodeForFiltering()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertNotEquals( FILTER, parseEncoding( reference ) );
            assertEquals( FILTER, parseEncoding( RelationshipReferenceEncoding.encodeForFiltering( reference ) ) );
            assertTrue( RelationshipReferenceEncoding.encodeForFiltering( reference ) < 0,
                    "encoded reference is negative" );
        }
    }

    @Test
    void encodeForTxStateFiltering()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertNotEquals( FILTER_TX_STATE, parseEncoding( reference ) );
            assertEquals( FILTER_TX_STATE, parseEncoding( RelationshipReferenceEncoding.encodeForTxStateFiltering( reference ) ) );
            assertTrue( RelationshipReferenceEncoding.encodeForTxStateFiltering( reference ) < 0,
                    "encoded reference is negative" );
        }
    }

    @Test
    void encodeFromGroup()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertNotEquals( GROUP, parseEncoding( reference ) );
            assertEquals( GROUP, parseEncoding( RelationshipReferenceEncoding.encodeGroup( reference ) ) );
            assertTrue( encodeGroup( reference ) < 0, "encoded reference is negative" );
        }
    }

    @Test
    void encodeNoIncomingRels()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            int token = random.nextInt(Integer.MAX_VALUE);
            assertNotEquals( NO_INCOMING_OF_TYPE, parseEncoding( token ) );
            assertEquals( NO_INCOMING_OF_TYPE, parseEncoding( RelationshipReferenceEncoding.encodeNoIncomingRels( token ) ) );
            assertTrue( RelationshipReferenceEncoding.encodeNoIncomingRels( token ) < 0,
                    "encoded reference is negative" );
        }
    }

    @Test
    void encodeNoOutgoingRels()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            int token = random.nextInt(Integer.MAX_VALUE);
            assertNotEquals( NO_OUTGOING_OF_TYPE, parseEncoding( token ) );
            assertEquals( NO_OUTGOING_OF_TYPE, parseEncoding( RelationshipReferenceEncoding.encodeNoOutgoingRels( token ) ) );
            assertTrue( RelationshipReferenceEncoding.encodeNoOutgoingRels( token ) < 0,
                    "encoded reference is negative" );
        }
    }

    @Test
    void encodeNoLoopRels()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            int token = random.nextInt(Integer.MAX_VALUE);
            assertNotEquals( NO_LOOP_OF_TYPE, parseEncoding( token ) );
            assertEquals( NO_LOOP_OF_TYPE, parseEncoding( RelationshipReferenceEncoding.encodeNoLoopRels( token ) ) );
            assertTrue( RelationshipReferenceEncoding.encodeNoLoopRels( token ) < 0, "encoded reference is negative" );
        }
    }

    // Group

    @Test
    void encodeRelationship()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertFalse( GroupReferenceEncoding.isRelationship( reference ) );
            assertTrue( GroupReferenceEncoding.isRelationship( GroupReferenceEncoding.encodeRelationship( reference ) ) );
            assertTrue( GroupReferenceEncoding.encodeRelationship( reference ) < 0, "encoded reference is negative" );
        }
    }
}
