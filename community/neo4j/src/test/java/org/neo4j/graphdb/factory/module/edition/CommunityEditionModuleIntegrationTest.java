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
package org.neo4j.graphdb.factory.module.edition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.function.Predicate;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.factory.DatabaseManagementServiceBuilder;
import org.neo4j.internal.id.BufferedIdController;
import org.neo4j.internal.id.BufferingIdGeneratorFactory;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( TestDirectoryExtension.class )
class CommunityEditionModuleIntegrationTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void createBufferedIdComponentsByDefault()
    {
        DatabaseManagementService managementService = new DatabaseManagementServiceBuilder( testDirectory.storeDir() ).build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
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
            managementService.shutdown();
        }
    }

    @Test
    void fileWatcherFileNameFilter()
    {
        DatabaseLayout layout = testDirectory.databaseLayout();
        Predicate<String> filter = CommunityEditionModule.communityFileWatcherFileNameFilter();
        assertFalse( filter.test( layout.metadataStore().getName() ) );
        assertFalse( filter.test( layout.nodeStore().getName() ) );
        assertTrue( filter.test( TransactionLogFilesHelper.DEFAULT_NAME + ".1" ) );
    }

}
