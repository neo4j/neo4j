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
package org.neo4j.io.proc;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ProcessUtilTest
{
    private static final String HELLO_WORLD = "Hello World";

    public static void main( String[] args )
    {
        System.out.println( HELLO_WORLD );
    }

    @Test
    public void mustFindWorkingJavaExecutableAndClassPath() throws Exception
    {
        List<String> command = new ArrayList<>();
        command.add( ProcessUtil.getJavaExecutable().toString() );
        command.add( "-cp" );
        command.add( ProcessUtil.getClassPath() );
        command.add( getClass().getName() );

        ProcessBuilder builder = new ProcessBuilder( command );
        Process process = builder.start();

        BufferedReader in = new BufferedReader( new InputStreamReader( process.getInputStream() ) );
        String line = in.readLine();

        assertThat( process.waitFor(), is( 0 ) );
        assertThat( line, equalTo( HELLO_WORLD ) );
    }
}
