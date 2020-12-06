/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.neo4j.shell.Historian;
import org.neo4j.shell.log.Logger;

import static java.lang.System.getProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class FileHistorianTest
{

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private Logger logger = mock( Logger.class );
    private InputStream mockedInput = mock( InputStream.class );
    private ConsoleReader reader = mock( ConsoleReader.class );

    @Before
    public void setup()
    {
        doReturn( System.out ).when( logger ).getOutputStream();
    }

    @Test
    public void defaultHistoryFile() throws Exception
    {
        Path expectedPath = Paths.get( getProperty( "user.home" ), ".neo4j", ".neo4j_history" );

        File history = FileHistorian.getDefaultHistoryFile();
        assertEquals( expectedPath.toString(), history.getPath() );
    }

    @Test
    public void noHistoryFileGivesMemoryHistory() throws Exception
    {
        File historyFile = Paths.get( temp.newFolder().getAbsolutePath(), "asfasd", "zxvses", "fanjtaacf" ).toFile();
        assertFalse( historyFile.getParentFile().isDirectory() );
        assertFalse( historyFile.getParentFile().getParentFile().isDirectory() );
        Historian historian = FileHistorian.setupHistory( reader, logger, historyFile );

        assertNotNull( historian );

        verify( logger ).printError( contains( "Could not load history file. Falling back to session-based history.\n" +
                                               "Failed to create directory for history" ) );
    }
}
