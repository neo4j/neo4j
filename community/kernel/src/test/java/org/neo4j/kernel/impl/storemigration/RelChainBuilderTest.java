/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class RelChainBuilderTest
{

    @Test
    public void shouldDetectCompleteChains() throws Exception
    {
        // Given
        RelChainBuilder chain = new RelChainBuilder( 1l );

        // When
        chain.append( new RelationshipRecord( 2l ), Record.NO_PREV_RELATIONSHIP.intValue(), 3l );
        chain.append( new RelationshipRecord( 3l ), 2l, 4l );
        chain.append( new RelationshipRecord( 4l ), 3l, Record.NO_NEXT_RELATIONSHIP.intValue() );

        // Then
        assertTrue( chain.isComplete() );
        assertThat( chain.size(), equalTo(3));
    }

    @Test
    public void shouldHandleArbitraryOrdering() throws Exception
    {
        // Given
        RelChainBuilder chain = new RelChainBuilder( 1l );

        // When
        chain.append( new RelationshipRecord( 2l ), 1l, 3l );
        chain.append( new RelationshipRecord( 4l ), 3l, Record.NO_NEXT_RELATIONSHIP.intValue() );
        chain.append( new RelationshipRecord( 3l ), 2l, 4l );
        chain.append( new RelationshipRecord( 1l ), Record.NO_PREV_RELATIONSHIP.intValue(), 2l );

        // Then
        assertTrue( chain.isComplete() );
        assertThat( chain.size(), equalTo(4));
    }
}
