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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;

public class StrategyResolverServiceTest
{

    StrategyResolverService subject;
    BackupStrategyWrapper haBackupStrategy;
    BackupStrategyWrapper ccBackupStrategy;

    @Before
    public void setup()
    {
        haBackupStrategy = mock( BackupStrategyWrapper.class );
        ccBackupStrategy = mock( BackupStrategyWrapper.class );
        subject = new StrategyResolverService( haBackupStrategy, ccBackupStrategy );
    }

    @Test
    public void anyProvidesBothStrategiesCorrectOrder()
    {
        List<BackupStrategyWrapper> result = subject.getStrategies( SelectedBackupProtocol.ANY );
        Assert.assertEquals( Arrays.asList( ccBackupStrategy, haBackupStrategy ), result );
    }

    @Test
    public void legacyProvidesBackupProtocol()
    {
        List<BackupStrategyWrapper> result = subject.getStrategies( SelectedBackupProtocol.COMMON );
        Assert.assertEquals( Collections.singletonList( haBackupStrategy ), result );
    }

    @Test
    public void catchupProvidesTransactionProtocol()
    {
        List<BackupStrategyWrapper> result = subject.getStrategies( SelectedBackupProtocol.CATCHUP );
        Assert.assertEquals( Collections.singletonList( ccBackupStrategy ), result );
    }
}
