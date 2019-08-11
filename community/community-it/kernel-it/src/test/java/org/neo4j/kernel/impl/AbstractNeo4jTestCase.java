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
package org.neo4j.kernel.impl;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.parallel.ResourceLock;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.extension.ExecutionSharedContext.SHARED_RESOURCE;

@ResourceLock( SHARED_RESOURCE )
public abstract class AbstractNeo4jTestCase
{
    private static DatabaseManagementService managementService;
    private static GraphDatabaseAPI graphDb;

    @BeforeAll
    static void beforeAll()
    {
        startDb();
    }

    @AfterAll
    static void afterAll()
    {
        stopDb();
    }

    protected static void startDb()
    {
        managementService = new TestDatabaseManagementServiceBuilder().impermanent().build();
        graphDb = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
    }

    protected static void stopDb()
    {
        managementService.shutdown();
    }

    public GraphDatabaseService getGraphDb()
    {
        return graphDb;
    }

    public DatabaseManagementService getManagementService()
    {
        return managementService;
    }

    protected GraphDatabaseAPI getGraphDbAPI()
    {
        return graphDb;
    }

    protected Node createNode()
    {
        Node node;
        try ( Transaction transaction = graphDb.beginTx() )
        {
            node = graphDb.createNode();
            transaction.commit();
        }
        return node;
    }

    protected IdGenerator getIdGenerator( IdType idType )
    {
        return graphDb.getDependencyResolver().resolveDependency( IdGeneratorFactory.class ).get( idType );
    }

    protected long propertyRecordsInUse()
    {
        return numberOfRecordsInUse( propertyStore() );
    }

    private static <RECORD extends AbstractBaseRecord> int numberOfRecordsInUse( RecordStore<RECORD> store )
    {
        int inUse = 0;
        for ( long id = store.getNumberOfReservedLowIds(); id < store.getHighId(); id++ )
        {
            RECORD record = store.getRecord( id, store.newRecord(), RecordLoad.FORCE );
            if ( record.inUse() )
            {
                inUse++;
            }
        }
        return inUse;
    }

    protected long dynamicStringRecordsInUse()
    {
        return numberOfRecordsInUse( propertyStore().getStringStore() );
    }

    protected long dynamicArrayRecordsInUse()
    {
        return numberOfRecordsInUse( propertyStore().getArrayStore() );
    }

    protected PropertyStore propertyStore()
    {
        return graphDb.getDependencyResolver().resolveDependency( RecordStorageEngine.class )
                .testAccessNeoStores().getPropertyStore();
    }
}
