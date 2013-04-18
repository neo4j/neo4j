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
package org.neo4j.server.rest.transactional.error;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.HashSet;

import org.junit.Test;

public class StatusCodeTest
{
    @Test
    public void eachStatusCodeHasAUniqueNumber() throws Exception
    {
        // given
        HashSet<Integer> numbers = new HashSet<Integer>();

        // when
        for ( StatusCode statusCode : StatusCode.values() )
        {
            numbers.add( statusCode.getCode() );
        }

        // then
        assertEquals( StatusCode.values().length, numbers.size() );
    }

    @Test
    public void eachStatusCodeHasADescription() throws Exception
    {
        // for each
        for ( StatusCode statusCode : StatusCode.values() )
        {
            StatusCodeDescription description = statusCode.getDescription();
            assertNotNull( statusCode.toString() + " description", description );
            assertNotNull( statusCode.toString() + " message", description.message() );
        }
    }
}
