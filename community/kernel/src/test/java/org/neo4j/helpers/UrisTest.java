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
package org.neo4j.helpers;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.Uris.parameter;

import java.net.URI;

import org.junit.Test;

public class UrisTest
{
    @Test
    public void testParam()
    {
        URI uri = URI.create("http://localhost/foo?x=a&y=b&z");

        assertThat( parameter( "x" ).apply( uri ), equalTo( "a" ) );
        assertThat( parameter( "y" ).apply( uri ), equalTo( "b" ) );
        assertThat( parameter( "z" ).apply( uri ), equalTo( "true" ) );
        assertThat( parameter( "foo" ).apply( uri ), nullValue() );
    }
}
