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
package org.neo4j.kernel.impl.transaction.log.pruning;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.File;
import java.time.Clock;
import java.util.stream.LongStream;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class LogPruningTest
{
    private final Config config = Config.defaults();
    private FileSystemAbstraction fs;
    private LogFiles logFiles;
    private LogProvider logProvider;
    private Clock clock;
    private LogPruneStrategyFactory factory;

    @Before
    public void setUp()
    {
        fs = mock( FileSystemAbstraction.class );
        logFiles = mock( LogFiles.class );
        doAnswer( inv -> new File( String.valueOf( inv.getArguments()[0] ) ) )
                .when( logFiles ).getLogFileForVersion( anyLong() );
        logProvider = NullLogProvider.getInstance();
        clock = mock( Clock.class );
        factory = mock( LogPruneStrategyFactory.class );
    }

    @Test
    public void mustDeleteLogFilesThatCanBePruned()
    {
        when( factory.strategyFromConfigValue( eq( fs ), eq( logFiles ), eq( clock ), anyString() ) )
                .thenReturn( upTo -> LongStream.range( 3, upTo ) );
        LogPruning pruning = new LogPruningImpl( fs, logFiles,logProvider,factory, clock, config );
        pruning.pruneLogs( 5 );
        InOrder order = inOrder( fs );
        order.verify( fs ).deleteFile( new File( "3" ) );
        order.verify( fs ).deleteFile( new File( "4" ) );
        // Log file 5 is not deleted; it's the lowest version expected to remain after pruning.
        verifyNoMoreInteractions( fs );
    }

    @Test
    public void mustHaveLogFilesToPruneIfStrategyFindsFiles()
    {
        when( factory.strategyFromConfigValue( eq( fs ), eq( logFiles ), eq( clock ), anyString() ) )
                .thenReturn(  upTo -> LongStream.range( 3, upTo ) );
        when( logFiles.getHighestLogVersion() ).thenReturn( 4L );
        LogPruning pruning = new LogPruningImpl( fs, logFiles, logProvider, factory, clock, config );
        assertTrue( pruning.mightHaveLogsToPrune() );
    }

    @Test
    public void mustNotHaveLogsFilesToPruneIfStrategyFindsNoFiles()
    {
        when( factory.strategyFromConfigValue( eq( fs ), eq( logFiles ), eq( clock ), anyString() ) )
                .thenReturn(  x -> LongStream.empty() );
        LogPruning pruning = new LogPruningImpl( fs, logFiles, logProvider, factory, clock, config );
        assertFalse( pruning.mightHaveLogsToPrune() );
    }
}
