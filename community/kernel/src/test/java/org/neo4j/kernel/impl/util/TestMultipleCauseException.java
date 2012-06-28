/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

import org.junit.Test;

public class TestMultipleCauseException
{
    @Test
    public void shouldBeAbleToAddCauses()
    {
        Throwable cause = new Throwable();
        MultipleCauseException exception = new MultipleCauseException( "Hello", cause );

        assertThat( exception.getMessage(), is( "Hello" ) );
        assertThat( exception.getCause(), is( cause ) );
        assertThat( exception.getCauses(), is( not( nullValue() ) ) );
        assertThat( exception.getCauses().size(), is( 1 ) );
        assertThat( exception.getCauses().get( 0 ), is( cause ) );
    }

    @Test
    public void stackTraceShouldContainAllCauses()
    {
        Throwable cause1 = new Throwable( "Message 1" );
        MultipleCauseException exception = new MultipleCauseException( "Hello", cause1 );

        Throwable cause2 = new Throwable( "Message 2" );
        exception.addCause( cause2 );

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintWriter out = new PrintWriter( baos );

        // When
        exception.printStackTrace( out );
        out.flush();
        String stackTrace = baos.toString();

        // Then
        assertThat( "Stack trace contains exception one as cause.",
                stackTrace.contains( "Caused by: java.lang.Throwable: Message 1" ), is( true ) );
        assertThat( "Stack trace contains exception one as cause.",
                stackTrace.contains( "Also caused by: java.lang.Throwable: Message 2" ), is( true ) );
    }
}
