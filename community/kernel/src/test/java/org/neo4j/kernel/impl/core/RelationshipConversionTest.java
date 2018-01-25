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
package org.neo4j.kernel.impl.core;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RelationshipConversionTest
{

    private EmbeddedProxySPI nodeActions = mock( EmbeddedProxySPI.class );
    private RelationshipConversion relationshipConversion;
    private RelationshipTraversalCursor cursor;

    @Before
    public void setUp()
    {
        when( nodeActions.newRelationshipProxy( anyLong(), anyLong(), anyInt(), anyLong() ) )
                .thenReturn( new RelationshipProxy( null, 1 ) );

        cursor = mock( RelationshipTraversalCursor.class );
        relationshipConversion = new RelationshipConversion( nodeActions, cursor, Direction.BOTH, null );
    }

    @Test
    public void closeStatementOnClose() throws Exception
    {
        when( cursor.next() ).thenReturn( false );

        relationshipConversion.close();

        verify( cursor ).close();
    }

    @Test
    public void closeStatementWhenIterationIsOver()
    {
        when( cursor.next() ).thenReturn( true, true, false );

        assertTrue( relationshipConversion.hasNext() );
        relationshipConversion.next();
        verify( cursor, never() ).close();

        assertTrue( relationshipConversion.hasNext() );
        relationshipConversion.next();
        verify( cursor, never() ).close();

        assertFalse( relationshipConversion.hasNext() );
        verify( cursor ).close();
    }

    @Test
    public void closeStatementOnlyOnce()
    {
        when( cursor.next() ).thenReturn( true, false );

        assertTrue( relationshipConversion.hasNext() );
        relationshipConversion.next();
        assertFalse( relationshipConversion.hasNext() );
        assertFalse( relationshipConversion.hasNext() );
        assertFalse( relationshipConversion.hasNext() );
        assertFalse( relationshipConversion.hasNext() );
        relationshipConversion.close();
        relationshipConversion.close();

        verify( cursor ).close();
    }
}
