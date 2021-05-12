/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.shell.cli;

import jline.console.ConsoleReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.shell.Historian;
import org.neo4j.shell.log.Logger;

import static java.lang.System.getProperty;
import static java.nio.file.Files.isDirectory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class FileHistorianTest
{
    private final Logger logger = mock( Logger.class );
    private final ConsoleReader reader = mock( ConsoleReader.class );

    @TempDir
    Path temp;

    @BeforeEach
    void setup()
    {
        doReturn( System.out ).when( logger ).getOutputStream();
    }

    @Test
    void defaultHistoryFile()
    {
        Path expectedPath = Paths.get( getProperty( "user.home" ), ".neo4j", ".neo4j_history" );

        File history = FileHistorian.getDefaultHistoryFile();
        assertEquals( expectedPath.toString(), history.getPath() );
    }

    @Test
    void noHistoryFileGivesMemoryHistory() throws Exception
    {
        Path historyFile = temp.resolve( Path.of( "asfasd", "zxvses", "fanjtaacf" ) );
        assertFalse( isDirectory( historyFile.getParent() ) );
        assertFalse( isDirectory( historyFile.getParent().getParent() ) );
        Historian historian = FileHistorian.setupHistory( reader, logger, historyFile.toFile() );

        assertNotNull( historian );

        verify( logger ).printError( contains( "Could not load history file. Falling back to session-based history.\n" +
                                               "Failed to create directory for history" ) );
    }
}
