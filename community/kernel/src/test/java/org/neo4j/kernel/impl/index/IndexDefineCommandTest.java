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
        int count = IndexDefineCommand.HIGHEST_POSSIBLE_ID;

        // When
        for ( int i = 0; i < count; i++ )
        {
            idc.getOrAssignKeyId( "key" + i );
            idc.getOrAssignIndexNameId( "index" + i );
        }

        // Then
        // it should break on too many
        try
        {
            idc.getOrAssignKeyId( "dropThatOverflows" );
            fail( "IndexDefineCommand should not allow more than " + count + " indexes per transaction" );
        }
        catch( IllegalStateException e )
        {
            // wonderful
        }

        try
        {
            idc.getOrAssignIndexNameId( "dropThatOverflows" );
            fail( "IndexDefineCommand should not allow more than " + count + " keys per transaction" );
        }
        catch( IllegalStateException e )
        {
            // wonderful
        }
    }
}
