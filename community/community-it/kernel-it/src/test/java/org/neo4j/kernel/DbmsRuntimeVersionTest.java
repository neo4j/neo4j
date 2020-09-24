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
package org.neo4j.kernel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

@TestDirectoryExtension
@DbmsExtension()
class DbmsRuntimeVersionTest
{
    @Inject
    private DatabaseManager<DatabaseContext> databaseManager;

    @Inject
    private DbmsRuntimeRepository dbmsRuntimeRepository;

    private GraphDatabaseService systemDb;

    @BeforeEach
    void beforeEach()
    {
        systemDb = databaseManager.getDatabaseContext( NAMED_SYSTEM_DATABASE_ID ).get().databaseFacade();
    }

    @Disabled( "Temporarily disabling until DBMS runtime system component is registered again" )
    @Test
    void testBasicVersionLifecycle()
    {
        // the system DB will be initialised with the default version for this binary
        assertSame( DbmsRuntimeVersion.V4_2, dbmsRuntimeRepository.getVersion() );

        // BTW this should never be manipulated directly outside tests
        setRuntimeVersion( DbmsRuntimeVersion.V4_1 );
        assertSame( DbmsRuntimeVersion.V4_1, dbmsRuntimeRepository.getVersion() );

        systemDb.executeTransactionally( "CALL dbms.upgrade()" );

        assertSame( DbmsRuntimeVersion.V4_2, dbmsRuntimeRepository.getVersion() );
    }

    private void setRuntimeVersion( DbmsRuntimeVersion runtimeVersion )
    {
        try ( var tx = systemDb.beginTx() )
        {
            tx.findNodes( DbmsRuntimeRepository.DBMS_RUNTIME_LABEL )
              .stream()
              .forEach( dbmsRuntimeNode -> dbmsRuntimeNode.setProperty( DbmsRuntimeRepository.VERSION_PROPERTY, runtimeVersion.getVersionNumber() ) );

            tx.commit();
        }

        dbmsRuntimeRepository.setVersion( runtimeVersion );
    }
}
