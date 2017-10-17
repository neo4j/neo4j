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
package org.neo4j.backup;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.causalclustering.LazySingletonSupplier;
import org.neo4j.causalclustering.SingleInstanceCore;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.mockito.Mockito.mock;

public class DeleteThisTest
{
    private final LazySingletonSupplier<TestDirectory> directoryLazySingletonSupplier = new LazySingletonSupplier<>( TestDirectory::testDirectory );

    @Rule
    public SingleInstanceCore singleInstanceCore = new SingleInstanceCore( directoryLazySingletonSupplier );

    @Rule
    public TestDirectory testDirectory = directoryLazySingletonSupplier.get();

    @Test
    public void canStartRule() throws CommandFailed
    {
        Config config  = singleInstanceCore.createCoreConfig( 56367 ); // TODO check ports again
        singleInstanceCore.start( config );

        // This backup should pass
        BackupStrategyCoordinatorBuilder builder =
                new BackupStrategyCoordinatorBuilder().withBackupDirectory( testDirectory.directory( "not/my/dir" ).toPath() );
        builder.fromSingleStrategy( BackupStrategyCoordinatorBuilder.StrategyEnum.CC ).performBackup( builder.getOnlineBackupContext() );
    }
}
