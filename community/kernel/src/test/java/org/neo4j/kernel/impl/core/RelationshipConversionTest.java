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

import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class RelationshipConversionTest
{

    private NodeProxy.NodeActions nodeActions = mock( NodeProxy.NodeActions.class );
    private Statement statement = mock( Statement.class );
    private RelationshipConversion relationshipConversion;

    @Before
    public void setUp()
    {
        relationshipConversion = new RelationshipConversion( nodeActions );
        relationshipConversion.statement = statement;
    }

    @Test
    public void closeStatementOnClose() throws Exception
    {
        relationshipConversion.close();

        verify( statement ).close();
    }

    @Test
    public void closeStatementWhenIterationIsOver()
    {
        relationshipConversion.iterator = new ArrayRelationshipVisitor( new long[]{1L, 8L} );

        assertTrue( relationshipConversion.hasNext() );
        relationshipConversion.next();
        verify( statement, never() ).close();

        assertTrue( relationshipConversion.hasNext() );
        relationshipConversion.next();
        verify( statement, never() ).close();

        assertFalse( relationshipConversion.hasNext() );
        verify( statement ).close();
    }

    @Test
    public void closeStatementOnlyOnce()
    {
        relationshipConversion.iterator = new ArrayRelationshipVisitor( new long[]{1L} );

        assertTrue( relationshipConversion.hasNext() );
        relationshipConversion.next();
        assertFalse( relationshipConversion.hasNext() );
        assertFalse( relationshipConversion.hasNext() );
        assertFalse( relationshipConversion.hasNext() );
        assertFalse( relationshipConversion.hasNext() );
        relationshipConversion.close();
        relationshipConversion.close();

        verify( statement ).close();
    }

    private static class ArrayRelationshipVisitor extends RelationshipIterator.BaseIterator
    {
        private final long[] ids;
        private int position;

        ArrayRelationshipVisitor(long[] ids)
        {
            this.ids = ids;
        }

        @Override
        protected boolean fetchNext()
        {
            return ids.length > position && next( ids[position++] );
        }

        @Override
        public <EXCEPTION extends Exception> boolean relationshipVisit( long relationshipId,
                RelationshipVisitor<EXCEPTION> visitor ) throws EXCEPTION
        {
            visitor.visit( relationshipId, 1, 1L, 1L );
            return false;
        }
    }
}
