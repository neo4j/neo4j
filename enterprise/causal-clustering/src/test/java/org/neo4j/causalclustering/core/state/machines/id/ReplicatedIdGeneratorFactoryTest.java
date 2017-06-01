/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state.machines.id;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.configuration.CommunityIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ReplicatedIdGeneratorFactoryTest
{
    @Rule
    public FileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();

    private NullLogProvider logProvider = NullLogProvider.getInstance();
    private FileSystemAbstraction fs;

    @Before
    public void setUp() throws Exception
    {
        fs = fileSystemRule.get();
    }

    @Test
    public void shouldCreateIdGenerator() throws Exception
    {
        ReplicatedIdGeneratorFactory replicatedIdGeneratorFactory = getReplicatedIdGeneratorFactory();

        File file = testDirectory.file( "id" );
        replicatedIdGeneratorFactory.open( file, IdType.NODE, 10, Long.MAX_VALUE );

        assertTrue( fs.fileExists( file ) );
    }

    @Test
    public void shouldReuseIdGenerator() throws Exception
    {
        ReplicatedIdGeneratorFactory replicatedIdGeneratorFactory = getReplicatedIdGeneratorFactory();

        File file = testDirectory.file( "id" );
        IdGenerator id1 = replicatedIdGeneratorFactory.open( file, IdType.NODE, 10, Long.MAX_VALUE );
        IdGenerator id2 = replicatedIdGeneratorFactory.open( file, IdType.NODE, 10, Long.MAX_VALUE );
        assertSame( id1, id2 );
    }

    private ReplicatedIdGeneratorFactory getReplicatedIdGeneratorFactory()
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = mock( ReplicatedIdRangeAcquirer.class );
        IdTypeConfigurationProvider idTypeConfigurationProvider = new CommunityIdTypeConfigurationProvider();
        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        return new ReplicatedIdGeneratorFactory( fs, rangeAcquirer, logProvider, idTypeConfigurationProvider,
                () -> false );
    }
}
