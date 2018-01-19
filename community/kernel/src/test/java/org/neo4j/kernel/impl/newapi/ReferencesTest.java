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

import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.kernel.impl.newapi.References.Group;
import org.neo4j.kernel.impl.newapi.References.Relationship;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.newapi.References.clearEncoding;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

public class ReferencesTest
{
    // This value the largest possible high limit id +1 (see HighLimitV3_1_0)
    private static long MAX_ID_LIMIT = 1L << 50;

    @Test
    public void shouldPreserveNoId()
    {
        assertThat( Relationship.encodeForFiltering( NO_ID ), equalTo( (long) NO_ID ) );
        assertThat( Relationship.encodeForTxStateFiltering( NO_ID ), equalTo( (long) NO_ID ) );
        assertThat( Relationship.encodeFromGroup( NO_ID ), equalTo( (long) NO_ID ) );
        assertThat( Relationship.encodeNoIncomingRels( NO_ID ), equalTo( (long) NO_ID ) );
        assertThat( Relationship.encodeNoOutgoingRels( NO_ID ), equalTo( (long) NO_ID ) );
        assertThat( Relationship.encodeNoLoopRels( NO_ID ), equalTo( (long) NO_ID ) );

        assertThat( Group.encodeRelationship( NO_ID ), equalTo( (long) NO_ID ) );
    }

    @Test
    public void shouldClearFlags()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            int token = random.nextInt(Integer.MAX_VALUE);

            assertThat( clearEncoding( Relationship.encodeFromGroup( reference ) ), equalTo( reference ) );
            assertThat( clearEncoding( Relationship.encodeForFiltering( reference ) ), equalTo( reference ) );
            assertThat( clearEncoding( Relationship.encodeForTxStateFiltering( reference ) ), equalTo( reference ) );
            assertThat( clearEncoding( Relationship.encodeNoIncomingRels( token ) ), equalTo( (long) token ) );
            assertThat( clearEncoding( Relationship.encodeNoOutgoingRels( token ) ), equalTo( (long) token ) );
            assertThat( clearEncoding( Relationship.encodeNoLoopRels( token ) ), equalTo( (long) token ) );

            assertThat( clearEncoding( Group.encodeRelationship( reference ) ), equalTo( reference ) );
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
            assertFalse( Relationship.isFilter( reference ) );
            assertTrue( Relationship.isFilter( Relationship.encodeForFiltering( reference ) ) );
            assertTrue( "encoded reference is negative", Relationship.encodeForFiltering( reference ) < 0 );
        }
    }

    @Test
    public void encodeForTxStateFiltering()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertFalse( Relationship.isTxStateFilter( reference ) );
            assertTrue( Relationship.isTxStateFilter( Relationship.encodeForTxStateFiltering( reference ) ) );
            assertTrue( "encoded reference is negative", Relationship.encodeForTxStateFiltering( reference ) < 0 );
        }
    }

    @Test
    public void encodeFromGroup()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertFalse( Relationship.isGroup( reference ) );
            assertTrue( Relationship.isGroup( Relationship.encodeFromGroup( reference ) ) );
            assertTrue( "encoded reference is negative", Relationship.encodeFromGroup( reference ) < 0 );
        }
    }

    @Test
    public void encodeNoIncomingRels()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            int token = random.nextInt(Integer.MAX_VALUE);
            assertFalse( Relationship.isNoIncoming( token ) );
            assertTrue( Relationship.isNoIncoming( Relationship.encodeNoIncomingRels( token ) ) );
            assertTrue( "encoded reference is negative", Relationship.encodeNoIncomingRels( token ) < 0 );
        }
    }

    @Test
    public void encodeNoOutgoingRels()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            int token = random.nextInt(Integer.MAX_VALUE);
            assertFalse( Relationship.isNoOutgoing( token ) );
            assertTrue( Relationship.isNoOutgoing( Relationship.encodeNoOutgoingRels( token ) ) );
            assertTrue( "encoded reference is negative", Relationship.encodeNoOutgoingRels( token ) < 0 );
        }
    }

    @Test
    public void encodeNoLoopRels()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            int token = random.nextInt(Integer.MAX_VALUE);
            assertFalse( Relationship.isNoLoop( token ) );
            assertTrue( Relationship.isNoLoop( Relationship.encodeNoLoopRels( token ) ) );
            assertTrue( "encoded reference is negative", Relationship.encodeNoLoopRels( token ) < 0 );
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
            assertFalse( Group.isRelationship( reference ) );
            assertTrue( Group.isRelationship( Group.encodeRelationship( reference ) ) );
            assertTrue( "encoded reference is negative", Group.encodeRelationship( reference ) < 0 );
        }
    }
}
