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

import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntToLongFunction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.kernel.api.Read.NO_ID;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.DENSE;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.DENSE_SELECTION;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.NO_INCOMING_OF_TYPE;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.NO_LOOPS_OF_TYPE;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.NO_OUTGOING_OF_TYPE;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.SELECTION;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.clearEncoding;
import static org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding.parseEncoding;

class ReferencesTest
{
    // This value the largest possible high limit id +1 (see HighLimitV3_1_0)
    private static final long MAX_ID_LIMIT = 1L << 50;

    @Test
    void shouldPreserveNoId()
    {
        assertThat( RelationshipReferenceEncoding.encodeDense( NO_ID ), equalTo( NO_ID ) );
        assertThat( RelationshipReferenceEncoding.encodeSelection( NO_ID ), equalTo( NO_ID ) );
        assertThat( RelationshipReferenceEncoding.encodeDenseSelection( NO_ID ), equalTo( NO_ID ) );
        assertThat( RelationshipReferenceEncoding.encodeNoIncoming( (int) NO_ID ), equalTo( NO_ID ) );
        assertThat( RelationshipReferenceEncoding.encodeNoOutgoing( (int) NO_ID ), equalTo( NO_ID ) );
        assertThat( RelationshipReferenceEncoding.encodeNoLoops( (int) NO_ID ), equalTo( NO_ID ) );
    }

    @Test
    void shouldClearFlags()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            int token = random.nextInt(Integer.MAX_VALUE);

            assertThat( clearEncoding( RelationshipReferenceEncoding.encodeDense( reference ) ), equalTo( reference ) );
            assertThat( clearEncoding( RelationshipReferenceEncoding.encodeSelection( reference ) ), equalTo( reference ) );
            assertThat( clearEncoding( RelationshipReferenceEncoding.encodeDenseSelection( reference ) ), equalTo( reference ) );
            assertThat( clearEncoding( RelationshipReferenceEncoding.encodeNoIncoming( token ) ), equalTo( (long) token ) );
            assertThat( clearEncoding( RelationshipReferenceEncoding.encodeNoOutgoing( token ) ), equalTo( (long) token ) );
            assertThat( clearEncoding( RelationshipReferenceEncoding.encodeNoLoops( token ) ), equalTo( (long) token ) );
        }
    }

    @Test
    void encodeDense()
    {
        testLongFlag( DENSE, RelationshipReferenceEncoding::encodeDense );
    }

    @Test
    void encodeSelection()
    {
        testLongFlag( SELECTION, RelationshipReferenceEncoding::encodeSelection );
    }

    @Test
    void encodeDenseSelection()
    {
        testLongFlag( DENSE_SELECTION, RelationshipReferenceEncoding::encodeDenseSelection );
    }

    @Test
    void encodeNoIncomingRels()
    {
        testIntFlag( NO_INCOMING_OF_TYPE, RelationshipReferenceEncoding::encodeNoIncoming );
    }

    @Test
    void encodeNoOutgoingRels()
    {
        testIntFlag( NO_OUTGOING_OF_TYPE, RelationshipReferenceEncoding::encodeNoOutgoing );
    }

    @Test
    void encodeNoLoopRels()
    {
        testIntFlag( NO_LOOPS_OF_TYPE, RelationshipReferenceEncoding::encodeNoLoops );
    }

    private void testLongFlag( RelationshipReferenceEncoding flag, LongToLongFunction encoder )
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertNotEquals( flag, parseEncoding( reference ) );
            assertEquals( flag, parseEncoding( encoder.applyAsLong( reference ) ) );
            assertTrue( encoder.applyAsLong( reference ) < 0, "encoded reference is negative" );
        }
    }

    private void testIntFlag( RelationshipReferenceEncoding flag, IntToLongFunction encoder )
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            int token = random.nextInt( Integer.MAX_VALUE );
            assertNotEquals( flag, parseEncoding( token ) );
            assertEquals( flag, parseEncoding( encoder.applyAsLong( token ) ) );
            assertTrue( encoder.applyAsLong( token ) < 0, "encoded reference is negative" );
        }
    }
}
