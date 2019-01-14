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
package org.neo4j.shell.impl;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import org.neo4j.shell.ShellClient;
import org.neo4j.test.rule.SuppressOutput;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class JLineConsoleTest
{

    @Rule
    public SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    @Test
    public void createAndUseJLineConsole()
    {
        ShellClient shellClient = Mockito.mock( ShellClient.class );
        JLineConsole jLineConsole = JLineConsole.newConsoleOrNullIfNotFound( shellClient );
        assertNotNull( "JLine should be available in classpath and it should be possible to use custom console.",
                jLineConsole );
        String testMessage = "The quick brown fox jumps over the lazy dog";
        jLineConsole.format( testMessage );
        assertTrue( suppressOutput.getOutputVoice().containsMessage( testMessage ) );
    }
}
