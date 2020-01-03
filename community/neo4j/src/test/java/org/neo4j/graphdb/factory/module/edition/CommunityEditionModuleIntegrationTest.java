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
package org.neo4j.graphdb.factory.module.edition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.function.Predicate;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.BufferedIdController;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.IdController;
import org.neo4j.kernel.impl.store.id.BufferingIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( TestDirectoryExtension.class )
class CommunityEditionModuleIntegrationTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void createBufferedIdComponentsByDefault()
    {
        GraphDatabaseAPI database = (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabase( testDirectory.storeDir() );
        try
        {
            DependencyResolver dependencyResolver = database.getDependencyResolver();
            IdController idController = dependencyResolver.resolveDependency( IdController.class );
            IdGeneratorFactory idGeneratorFactory = dependencyResolver.resolveDependency( IdGeneratorFactory.class );

            assertThat( idController, instanceOf( BufferedIdController.class ) );
            assertThat( idGeneratorFactory, instanceOf( BufferingIdGeneratorFactory.class ) );
        }
        finally
        {
            database.shutdown();
        }
    }

    @Test
    void fileWatcherFileNameFilter()
    {
        DatabaseLayout layout = testDirectory.databaseLayout();
        Predicate<String> filter = CommunityEditionModule.communityFileWatcherFileNameFilter();
        assertFalse( filter.test( layout.metadataStore().getName() ) );
        assertFalse( filter.test( layout.nodeStore().getName() ) );
        assertTrue( filter.test( TransactionLogFiles.DEFAULT_NAME + ".1" ) );
        assertTrue( filter.test( IndexConfigStore.INDEX_DB_FILE_NAME + ".any" ) );
    }

}
