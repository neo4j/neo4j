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
package org.neo4j.kernel.impl.proc;

import org.junit.Test;

import java.util.NoSuchElementException;

import org.neo4j.internal.kernel.api.procs.QualifiedName;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ProcedureHolderTest
{
    @Test
    public void shouldGetProcedureFromHolder()
    {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qualifiedName = new QualifiedName( new String[0], "CaseSensitive" );
        String item = "CaseSensitiveItem";
        procHolder.put( qualifiedName, item, false );

        // then
        assertThat( procHolder.get( qualifiedName ), equalTo( item ) );
        assertThat( procHolder.idOf( qualifiedName ), equalTo( 0 ) );
    }

    @Test
    public void okToHaveProcsOnlyDifferByCase()
    {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        procHolder.put( new QualifiedName( new String[0], "CASESENSITIVE" ), "CASESENSITIVEItem", false );
        procHolder.put( new QualifiedName( new String[0], "CaseSensitive" ), "CaseSensitiveItem", false );

        // then
        assertThat( procHolder.get( new QualifiedName( new String[0], "CASESENSITIVE" ) ), equalTo( "CASESENSITIVEItem" ) );
        assertThat( procHolder.get( new QualifiedName( new String[0], "CaseSensitive" ) ), equalTo( "CaseSensitiveItem" ) );
        assertThat( procHolder.idOf( new QualifiedName( new String[0], "CASESENSITIVE" ) ), equalTo( 0 ) );
        assertThat( procHolder.idOf( new QualifiedName( new String[0], "CaseSensitive" ) ), equalTo( 1 ) );
    }

    @Test
    public void shouldGetCaseInsensitiveFromHolder()
    {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qualifiedName = new QualifiedName( new String[0], "CaseInSensitive" );
        String item = "CaseInSensitiveItem";
        procHolder.put( qualifiedName, item, true );

        // then
        QualifiedName lowerCaseName = new QualifiedName( new String[0], "caseinsensitive" );
        assertThat( procHolder.get( lowerCaseName ), equalTo( item ) );
        assertThat( procHolder.idOf( lowerCaseName ), equalTo( 0 ) );
    }

    @Test
    public void canOverwriteFunctionAndChangeCaseSensitivity()
    {
        // given
        ProcedureHolder<String> procHolder = new ProcedureHolder<>();
        QualifiedName qualifiedName = new QualifiedName( new String[0], "CaseInSensitive" );
        String item = "CaseInSensitiveItem";
        procHolder.put( qualifiedName, item, true );

        // then
        QualifiedName lowerCaseName = new QualifiedName( new String[0], "caseinsensitive" );
        assertThat( procHolder.get( lowerCaseName ), equalTo( item ) );
        assertThat( procHolder.idOf( lowerCaseName ), equalTo( 0 ) );

        // and then
        procHolder.put( qualifiedName, item, false );
        assertNull( procHolder.get( lowerCaseName ) );
        try
        {
            procHolder.idOf( lowerCaseName );
            fail( "Should have failed to find with lower case" );
        }
        catch ( NoSuchElementException e )
        {
            // expected
        }
    }
}
