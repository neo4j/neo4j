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
package org.neo4j.kernel.impl.factory;

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.ReadOnlyDatabaseChecker;
import org.neo4j.kernel.impl.api.DatabaseTransactionCommitProcess;
import org.neo4j.kernel.impl.api.InternalTransactionCommitProcess;
import org.neo4j.kernel.impl.api.ReadOnlyTransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.storageengine.api.StorageEngine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

class CommunityCommitProcessFactoryTest
{
    @Test
    void createReadOnlyCommitProcessWhenFixedReadOnly()
    {
        var factory = new CommunityCommitProcessFactory();

        var alwaysReadOnly = new ReadOnlyDatabaseChecker()
        {
            @Override
            public boolean test( String databaseName )
            {
                return true;
            }

            @Override
            public boolean readOnlyFixed()
            {
                return true;
            }
        };

        var commitProcess = factory.create( mock( TransactionAppender.class ),
                                            mock( StorageEngine.class ),
                                            randomNamedDatabaseId(),
                                            alwaysReadOnly );

        assertThat( commitProcess ).isInstanceOf( ReadOnlyTransactionCommitProcess.class );
    }

    @Test
    void createRegularCommitProcessWhenWritable()
    {
        var factory = new CommunityCommitProcessFactory();
        ReadOnlyDatabaseChecker neverReadOnly = databaseName -> false;

        var commitProcess = factory.create( mock( TransactionAppender.class ),
                                                                 mock( StorageEngine.class ),
                                                                 randomNamedDatabaseId(),
                                                                 neverReadOnly );

        assertThat( commitProcess ).isInstanceOf( DatabaseTransactionCommitProcess.class );
    }

    @Test
    void createRegularCommitProcessWhenDynamicallyReadOnly()
    {
        var factory = new CommunityCommitProcessFactory();
        ReadOnlyDatabaseChecker readOnly = databaseName -> true;

        var commitProcess = factory.create( mock( TransactionAppender.class ),
                                            mock( StorageEngine.class ),
                                            randomNamedDatabaseId(),
                                            readOnly );

        assertThat( commitProcess ).isInstanceOf( DatabaseTransactionCommitProcess.class );
    }
}
