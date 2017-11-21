/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.newapi.References.clearFlags;
import static org.neo4j.kernel.impl.newapi.References.hasDirectFlag;
import static org.neo4j.kernel.impl.newapi.References.hasFilterFlag;
import static org.neo4j.kernel.impl.newapi.References.hasGroupFlag;
import static org.neo4j.kernel.impl.newapi.References.hasNodeFlag;
import static org.neo4j.kernel.impl.newapi.References.hasRelationshipFlag;
import static org.neo4j.kernel.impl.newapi.References.hasTxStateFlag;
import static org.neo4j.kernel.impl.newapi.References.setDirectFlag;
import static org.neo4j.kernel.impl.newapi.References.setFilterFlag;
import static org.neo4j.kernel.impl.newapi.References.setGroupFlag;
import static org.neo4j.kernel.impl.newapi.References.setNodeFlag;
import static org.neo4j.kernel.impl.newapi.References.setRelationshipFlag;
import static org.neo4j.kernel.impl.newapi.References.setTxStateFlag;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

public class ReferencesTest
{
    // This value the largest possible high limit id +1 (see HighLimitV3_1_0)
    private static long MAX_ID_LIMIT = 1L << 50;

    @Test
    public void shouldPreserveNoId()
    {
        assertThat( setDirectFlag( NO_ID ), equalTo( (long) NO_ID ) );
        assertThat( setFilterFlag( NO_ID ), equalTo( (long) NO_ID ) );
        assertThat( setGroupFlag( NO_ID ), equalTo( (long) NO_ID ) );
    }

    @Test
    public void shouldClearFlags()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertThat( clearFlags( setDirectFlag( reference ) ), equalTo( reference ) );
            assertThat( clearFlags( setGroupFlag( reference ) ), equalTo( reference ) );
            assertThat( clearFlags( setFilterFlag( reference ) ), equalTo( reference ) );
        }
    }

    @Test
    public void shouldSetFilterFlag()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertFalse( hasFilterFlag( reference ) );
            assertTrue( hasFilterFlag( setFilterFlag( reference ) ) );
        }
    }

    @Test
    public void shouldSetDirectFlag()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertFalse( hasDirectFlag( reference ) );
            assertTrue( hasDirectFlag( setDirectFlag( reference ) ) );
        }
    }

    @Test
    public void shouldSetGroupFlag()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertFalse( hasGroupFlag( reference ) );
            assertTrue( hasGroupFlag( setGroupFlag( reference ) ) );
        }
    }

    @Test
    public void shouldSetTxStateFlag()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertFalse( hasTxStateFlag( reference ) );
            assertTrue( hasTxStateFlag( setTxStateFlag( reference ) ) );
        }
    }

    @Test
    public void shouldSetNodeFlag()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertFalse( hasNodeFlag( reference ) );
            assertTrue( hasNodeFlag( setNodeFlag( reference ) ) );
        }
    }

    @Test
    public void shouldSetRelationshipFlag()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for ( int i = 0; i < 1000; i++ )
        {
            long reference = random.nextLong( MAX_ID_LIMIT );
            assertFalse( hasRelationshipFlag( reference ) );
            assertTrue( hasRelationshipFlag( setRelationshipFlag( reference ) ) );
        }
    }
}
