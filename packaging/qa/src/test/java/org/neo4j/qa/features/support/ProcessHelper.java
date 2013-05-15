/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.qa.features.support;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProcessHelper
{
    public static void exec( File directory, String program, String... arguments ) throws IOException, InterruptedException
    {
        List<String> programPlusArgs = new ArrayList<String>( asList( program ) );
        programPlusArgs.addAll( asList( arguments ) );

        int resultCode = new ProcessBuilder( programPlusArgs )
                .directory( directory )
                .start().waitFor();

        assertEquals( programPlusArgs.toString(), 0, resultCode );
    }
}
