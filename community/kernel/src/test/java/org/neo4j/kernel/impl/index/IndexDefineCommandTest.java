/*
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
package org.neo4j.kernel.impl.index;

import org.junit.Test;

import static org.junit.Assert.fail;

public class IndexDefineCommandTest
{
    @Test
    public void testIndexCommandCreationEnforcesLimit() throws Exception
    {
        // Given
        IndexDefineCommand idc = new IndexDefineCommand();
        int max = (int) (Math.pow( 2, 16 ) - 1);

        // When
        for ( int i = 0; i < max; i++ )
        {
            idc.getOrAssignKeyId( "index" + i );
        }

        // Then
        // it should break on the 64th
        try
        {
            idc.getOrAssignKeyId( "dropThatOverflows" );
            fail( "IndexDefineCommand should not allow more than 63 indexes per transaction" );
        }
        catch( IllegalStateException e )
        {
            // wonderful
        }
    }
}
