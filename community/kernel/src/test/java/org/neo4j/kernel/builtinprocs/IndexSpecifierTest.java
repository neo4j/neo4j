/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.builtinprocs;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

public class IndexSpecifierTest
{
    @Test
    public void shouldFormatAsCanonicalRepresentation()
    {
        assertThat( new IndexSpecifier( ":Person(name)" ).toString(), is( ":Person(name)" ) );
    }

    @Test
    public void shouldParseASimpleLabel()
    {
        assertThat( new IndexSpecifier( ":Person_23(name)" ).label(), is( "Person_23" ) );
    }

    @Test
    public void shouldParseASimpleProperty()
    {
        assertThat( new IndexSpecifier( ":Person(a_Name_123)" ).property(), is( "a_Name_123" ) );
    }

    @Test
    public void shouldParseANastyLabel()
    {
        assertThat( new IndexSpecifier( ":`:(!\"£$%^&*( )`(name)" ).label(), is( ":(!\"£$%^&*( )" ) );
    }

    @Test
    public void shouldParseANastyProperty()
    {
        assertThat( new IndexSpecifier( ":Person(`(:!\"£$%^&*( )`)" ).property(), is( "(:!\"£$%^&*( )" ) );
    }

    @Test
    public void shouldProduceAReasonableErrorIfTheSpecificationCantBeParsed()
    {
        try
        {
            new IndexSpecifier( "rubbish" );
            fail( "expected exception" );
        }
        catch ( IllegalArgumentException e )
        {
            //expected
        }
    }
}
