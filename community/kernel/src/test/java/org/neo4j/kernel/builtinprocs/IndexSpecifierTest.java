/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class IndexSpecifierTest
{
    @Test
    public void shouldFormatAsCanonicalRepresentation()
    {
        assertThat( IndexSpecifier.byPatternOrName( ":Person(name)" ).toString(), is( ":Person(name)" ) );
        assertThat( IndexSpecifier.byPattern( ":Person(name)" ).toString(), is( ":Person(name)" ) );
    }

    @Test
    public void shouldParseASimpleLabel()
    {
        assertThat( IndexSpecifier.byPatternOrName( ":Person_23(name)" ).label(), is( "Person_23" ) );
        assertThat( IndexSpecifier.byPattern( ":Person_23(name)" ).label(), is( "Person_23" ) );
    }

    @Test
    public void shouldParseASimpleProperty()
    {
        assertThat( IndexSpecifier.byPatternOrName( ":Person(a_Name_123)" ).properties(), is( arrayContaining( "a_Name_123" ) ) );
        assertThat( IndexSpecifier.byPattern( ":Person(a_Name_123)" ).properties(), is( arrayContaining( "a_Name_123" ) ) );
    }

    @Test
    public void shouldParseTwoProperties()
    {
        assertThat( IndexSpecifier.byPatternOrName( ":Person(name, lastName)" ).properties(),
                is( arrayContaining( "name", "lastName" ) ) );
        assertThat( IndexSpecifier.byPattern( ":Person(name, lastName)" ).properties(),
                is( arrayContaining( "name", "lastName" ) ) );
    }

    @Test
    public void shouldParseManyProperties()
    {
        assertThat( IndexSpecifier.byPatternOrName( ":Person(1, 2, 3, 4, 5, 6)" ).properties(),
                is( arrayContaining( "1", "2", "3", "4", "5", "6" ) ) );
        assertThat( IndexSpecifier.byPattern( ":Person(1, 2, 3, 4, 5, 6)" ).properties(),
                is( arrayContaining( "1", "2", "3", "4", "5", "6" ) ) );
    }

    @Test
    public void shouldParseManyPropertiesWithWhitespace()
    {
        String specification = ":Person( 1 , 2   ,3   ,4  )";
        assertThat( IndexSpecifier.byPatternOrName( specification ).properties(),
                is( arrayContaining( "1", "2", "3", "4" ) ) );
        assertThat( IndexSpecifier.byPattern( specification ).properties(),
                is( arrayContaining( "1", "2", "3", "4" ) ) );
    }

    @Test
    public void shouldParseOddProperties()
    {
        assertThat( IndexSpecifier.byPatternOrName( ": Person(1,    2lskgj_LKHGS, `3sdlkhs,   df``sas;g`, 4, `  5  `, 6)" ).properties(),
                is( arrayContaining( "1", "2lskgj_LKHGS", "3sdlkhs,   df``sas;g", "4", "  5  ", "6" ) ) );
        assertThat( IndexSpecifier.byPattern( ": Person(1,    2lskgj_LKHGS, `3sdlkhs,   df``sas;g`, 4, `  5  `, 6)" ).properties(),
                is( arrayContaining( "1", "2lskgj_LKHGS", "3sdlkhs,   df``sas;g", "4", "  5  ", "6" ) ) );
    }

    @Test
    public void shouldParseANastyLabel()
    {
        assertThat( IndexSpecifier.byPatternOrName( ":`:(!\"£$%^&*( )`(name)" ).label(), is( ":(!\"£$%^&*( )" ) );
        assertThat( IndexSpecifier.byPattern( ":`:(!\"£$%^&*( )`(name)" ).label(), is( ":(!\"£$%^&*( )" ) );
    }

    @Test
    public void shouldParseANastyProperty()
    {
        assertThat( IndexSpecifier.byPatternOrName( ":Person(`(:!\"£$%^&*( )`)" ).properties(),
                is( arrayContaining( "(:!\"£$%^&*( )" ) ) );
        assertThat( IndexSpecifier.byPattern( ":Person(`(:!\"£$%^&*( )`)" ).properties(),
                is( arrayContaining( "(:!\"£$%^&*( )" ) ) );
    }

    @Test
    public void specifiersThatDoNotBeginWithColonAreIndexNames()
    {
        IndexSpecifier spec = IndexSpecifier.byPatternOrName( "my_index" );
        assertThat( spec.name(), is( "my_index" ) );
        assertNull( spec.label() );
        assertNull( spec.properties() );

        spec = IndexSpecifier.byName( "my_index" );
        assertThat( spec.name(), is( "my_index" ) );
        assertNull( spec.label() );
        assertNull( spec.properties() );
    }

    @Test
    public void patternSpecifiersHaveNoName()
    {
        IndexSpecifier spec = IndexSpecifier.byPattern( ":Person(name)" );
        assertNotNull( spec.label() );
        assertNotNull( spec.properties() );
        assertNull( spec.name() );
    }

    @Test
    public void shouldProduceAReasonableErrorIfTheSpecificationCantBeParsed()
    {
        try
        {
            IndexSpecifier.byPatternOrName( "just some rubbish" );
            fail( "expected exception" );
        }
        catch ( IllegalArgumentException e )
        {
            //expected
        }

        try
        {
            IndexSpecifier.byPattern( "rubbish" );
            fail( "expected exception" );
        }
        catch ( IllegalArgumentException e )
        {
            //expected
        }

        try
        {
            IndexSpecifier.byName( ":Person(name)" );
            fail( "expected exception" );
        }
        catch ( IllegalArgumentException e )
        {
            //expected
        }
    }
}
