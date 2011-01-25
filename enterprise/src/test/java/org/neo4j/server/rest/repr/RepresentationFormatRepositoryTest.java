/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest.repr;

import org.junit.Test;

import javax.ws.rs.core.MediaType;

import static org.junit.Assert.fail;

public class RepresentationFormatRepositoryTest
{
    private final RepresentationFormatRepository repository = new RepresentationFormatRepository(
            null );

    @Test
    public void canProvideJsonFormat() throws Exception
    {
        repository.inputFormat( MediaType.valueOf( "application/json" ) );
    }

    @Test
    public void canProvideUTF8EncodedJsonFormat() throws Exception
    {
        repository.inputFormat( MediaType.valueOf( "application/json;charset=UTF-8" ) );
    }

    @Test( expected = MediaTypeNotSupportedException.class )
    public void canNotGetInputFormatBasedOnWildcardMediaType() throws Exception
    {
        InputFormat format = repository.inputFormat( MediaType.WILDCARD_TYPE );
        format.readValue( "foo" );
        fail( "Got InputFormat based on wild card type: " + format );
    }
}
