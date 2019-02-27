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
package org.neo4j.procedure.builtin;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.procedure.builtin.IndexSpecifier.byPattern;
import static org.neo4j.procedure.builtin.IndexSpecifier.byPatternOrName;

class IndexSpecifierTest
{
    @Test
    void shouldFormatAsCanonicalRepresentation()
    {
        assertThat( byPatternOrName( ":Person(name)" ).toString(), is( ":Person(name)" ) );
        assertThat( byPattern( ":Person(name)" ).toString(), is( ":Person(name)" ) );
    }

    @Test
    void shouldParseASimpleLabel()
    {
        assertThat( byPatternOrName( ":Person_23(name)" ).label(), is( "Person_23" ) );
        assertThat( byPattern( ":Person_23(name)" ).label(), is( "Person_23" ) );
    }

    @Test
    void shouldParseASimpleProperty()
    {
        assertThat( byPatternOrName( ":Person(a_Name_123)" ).properties(), is( arrayContaining( "a_Name_123" ) ) );
        assertThat( byPattern( ":Person(a_Name_123)" ).properties(), is( arrayContaining( "a_Name_123" ) ) );
    }

    @Test
    void shouldParseTwoProperties()
    {
        assertThat( byPatternOrName( ":Person(name, lastName)" ).properties(),
                is( arrayContaining( "name", "lastName" ) ) );
        assertThat( byPattern( ":Person(name, lastName)" ).properties(),
                is( arrayContaining( "name", "lastName" ) ) );
    }

    @Test
    void shouldParseManyProperties()
    {
        assertThat( byPatternOrName( ":Person(1, 2, 3, 4, 5, 6)" ).properties(),
                is( arrayContaining( "1", "2", "3", "4", "5", "6" ) ) );
        assertThat( byPattern( ":Person(1, 2, 3, 4, 5, 6)" ).properties(),
                is( arrayContaining( "1", "2", "3", "4", "5", "6" ) ) );
    }

    @Test
    void shouldParseManyPropertiesWithWhitespace()
    {
        String specification = ":Person( 1 , 2   ,3   ,4  )";
        assertThat( byPatternOrName( specification ).properties(),
                is( arrayContaining( "1", "2", "3", "4" ) ) );
        assertThat( byPattern( specification ).properties(),
                is( arrayContaining( "1", "2", "3", "4" ) ) );
    }

    @Test
    void shouldParseOddProperties()
    {
        assertThat( byPatternOrName( ": Person(1,    2lskgj_LKHGS, `3sdlkhs,   df``sas;g`, 4, `  5  `, 6)" ).properties(),
                is( arrayContaining( "1", "2lskgj_LKHGS", "3sdlkhs,   df``sas;g", "4", "  5  ", "6" ) ) );
        assertThat( byPattern( ": Person(1,    2lskgj_LKHGS, `3sdlkhs,   df``sas;g`, 4, `  5  `, 6)" ).properties(),
                is( arrayContaining( "1", "2lskgj_LKHGS", "3sdlkhs,   df``sas;g", "4", "  5  ", "6" ) ) );
    }

    @Test
    void shouldParseANastyLabel()
    {
        assertThat( byPatternOrName( ":`:(!\"£$%^&*( )`(name)" ).label(), is( ":(!\"£$%^&*( )" ) );
        assertThat( byPattern( ":`:(!\"£$%^&*( )`(name)" ).label(), is( ":(!\"£$%^&*( )" ) );
    }

    @Test
    void shouldParseANastyProperty()
    {
        assertThat( byPatternOrName( ":Person(`(:!\"£$%^&*( )`)" ).properties(),
                is( arrayContaining( "(:!\"£$%^&*( )" ) ) );
        assertThat( byPattern( ":Person(`(:!\"£$%^&*( )`)" ).properties(),
                is( arrayContaining( "(:!\"£$%^&*( )" ) ) );
    }

    @Test
    void specifiersThatDoNotBeginWithColonAreIndexNames()
    {
        IndexSpecifier spec = byPatternOrName( "my_index" );
        assertThat( spec.name(), is( "my_index" ) );
        assertNull( spec.label() );
        assertNull( spec.properties() );

        spec = IndexSpecifier.byName( "my_index" );
        assertThat( spec.name(), is( "my_index" ) );
        assertNull( spec.label() );
        assertNull( spec.properties() );
    }

    @Test
    void patternSpecifiersHaveNoName()
    {
        IndexSpecifier spec = byPattern( ":Person(name)" );
        assertNotNull( spec.label() );
        assertNotNull( spec.properties() );
        assertNull( spec.name() );
    }

    @Test
    void shouldProduceAReasonableErrorIfTheSpecificationCantBeParsed()
    {
        assertThrows( IllegalArgumentException.class, () -> byPatternOrName( "just some rubbish" ) );
        assertThrows( IllegalArgumentException.class, () -> byPattern( "rubbish" ) );
        assertThrows( IllegalArgumentException.class, () -> IndexSpecifier.byName( ":Person(name)" ) );
    }
}
