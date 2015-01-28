/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class InputEntityTest
{
    @Test
    public void shouldAddProperties() throws Exception
    {
        // GIVEN
        InputNode node = new InputNode( "id", new Object[] {
                "first", "Yeah",
                "second", "Yo"
        }, null, InputEntity.NO_LABELS, null );

        // WHEN
        node.addProperties( "third", "Yee" );

        // THEN
        assertArrayEquals( new Object[] {
                "first", "Yeah",
                "second", "Yo",
                "third", "Yee"
        }, node.properties() );
    }

    @Test
    public void shouldSetProperties() throws Exception
    {
        // GIVEN
        InputNode node = new InputNode( "id", new Object[] {
                "first", "Yeah",
                "second", "Yo"
        }, null, InputEntity.NO_LABELS, null );

        // WHEN
        node.setProperties( "third", "Yee" );

        // THEN
        assertArrayEquals( new Object[] { "third", "Yee" }, node.properties() );
    }
}
