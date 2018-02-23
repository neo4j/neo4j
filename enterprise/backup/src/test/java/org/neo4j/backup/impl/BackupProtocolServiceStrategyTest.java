/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.backup.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import org.neo4j.backup.IncrementalBackupNotPossibleException;
import org.neo4j.com.ComException;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.OptionalHostnamePort;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.backup.impl.BackupStageOutcome.SUCCESS;

public class BackupProtocolServiceStrategyTest
{
    private BackupProtocolService backupProtocolService = mock( BackupProtocolService.class );

    HaBackupStrategy subject;

    Config config = mock( Config.class );
    OnlineBackupRequiredArguments requiredArgs = mock( OnlineBackupRequiredArguments.class );
    OnlineBackupContext onlineBackupContext = mock( OnlineBackupContext.class );
    AddressResolver addressResolver = mock( AddressResolver.class );
    HostnamePort hostnamePort = new HostnamePort( "hostname:1234" );
    Path backupDirectory = mock( Path.class );
    OptionalHostnamePort userSpecifiedHostname = new OptionalHostnamePort( (String) null, null, null );

    @BeforeEach
    public void setup()
    {
        when( onlineBackupContext.getRequiredArguments() ).thenReturn( requiredArgs );
        when( addressResolver.resolveCorrectHAAddress( any(), any() ) ).thenReturn( hostnamePort );
        subject = new HaBackupStrategy( backupProtocolService, addressResolver, 0 );
    }

    @Test
    public void incrementalBackupsAreDoneAgainstResolvedAddress()
    {
        // when
        Fallible<BackupStageOutcome> state = subject.performIncrementalBackup(
                backupDirectory, config, userSpecifiedHostname );

        // then
        verify( backupProtocolService ).doIncrementalBackup( eq( hostnamePort.getHost() ),
                eq( hostnamePort.getPort() ), any(), eq( ConsistencyCheck.NONE ), anyLong(), any() );
        assertEquals( SUCCESS, state.getState() );
    }

    @Test
    public void exceptionsDuringIncrementalBackupAreMarkedAsFailedBackups()
    {
        // given incremental backup will fail
        IncrementalBackupNotPossibleException expectedException = new IncrementalBackupNotPossibleException(
                "Expected test message", new RuntimeException( "Expected cause" ) );
        when( backupProtocolService.doIncrementalBackup( any(), anyInt(), any(), eq( ConsistencyCheck.NONE ), anyLong(), any() ) )
            .thenThrow( expectedException );

        // when
        Fallible state = subject.performIncrementalBackup( backupDirectory, config, userSpecifiedHostname );

        // then
        assertEquals( BackupStageOutcome.FAILURE, state.getState() );
        assertEquals( expectedException, state.getCause().get() );
    }

    @Test
    public void fullBackupUsesResolvedAddress()
    {
        // when
        Fallible state = subject.performFullBackup( backupDirectory, config, userSpecifiedHostname );

        // then
        verify( backupProtocolService ).doFullBackup( any(), anyInt(), any(), eq( ConsistencyCheck.NONE ), any(), anyLong(), anyBoolean() );
        assertEquals( BackupStageOutcome.SUCCESS, state.getState() );
    }

    @Test
    public void fullBackupFailsWithCauseOnException()
    {
        // given full backup fails with a protocol/network exception
        when( backupProtocolService.doFullBackup( any(), anyInt(), any(), any(), any(), anyLong(), anyBoolean() ) )
                .thenThrow( ComException.class );

        // when
        Fallible state = subject.performFullBackup( backupDirectory, config, userSpecifiedHostname );

        // then
        assertEquals( BackupStageOutcome.WRONG_PROTOCOL, state.getState() );
        assertEquals( ComException.class, state.getCause().get().getClass() );
    }
}
