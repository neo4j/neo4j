/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.util.NoSuchElementException;

import org.junit.Test;

import static java.util.Arrays.asList;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.Neo4jMatchers.hasSamePrimitiveItems;

public class PrimitiveLongIteratorForArrayTest
{
    @Test
    public void shouldIterateEmptyArray() throws Exception
    {
        // given
        PrimitiveLongIterator iterator =  new PrimitiveLongIteratorForArray();

        // when
        assertFalse( "should not have next element", iterator.hasNext() );

        try
        {
            iterator.next();
            fail("Expected NoSuchElementException");
        }
        catch ( NoSuchElementException e )
        {
            assertNull( e.getMessage() );
        }
    }

    @Test
    public void shouldIterateNonEmptyArray() throws Exception
    {
        // given
        PrimitiveLongIterator primitiveLongs =  new PrimitiveLongIteratorForArray( 42l, 23l );

        // when
        assertThat( asList( 42l, 23l ).iterator(), hasSamePrimitiveItems( primitiveLongs) );
    }
}
