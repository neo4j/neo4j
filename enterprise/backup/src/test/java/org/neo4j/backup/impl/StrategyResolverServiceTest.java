/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
