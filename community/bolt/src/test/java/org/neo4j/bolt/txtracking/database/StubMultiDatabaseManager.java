/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.internal.NullLogService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class StubMultiDatabaseManager extends MultiDatabaseManager<DatabaseContext>
{
    private static final GlobalModule globalModule = mockGlobalModule();

    public StubMultiDatabaseManager()
    {
        super( globalModule, null, true );
    }

    @Override
    protected DatabaseContext createDatabaseContext( DatabaseId databaseId )
    {
        Database db = mock( Database.class );
        when( db.getDatabaseId() ).thenReturn( databaseId );
        return spy( new StandaloneDatabaseContext( db ) );
    }

    private static GlobalModule mockGlobalModule()
    {
        Dependencies dependencies = new Dependencies();
        GlobalModule module = mock( GlobalModule.class );
        when( module.getGlobalDependencies() ).thenReturn( dependencies );
        when( module.getGlobalConfig() ).thenReturn( Config.defaults() );
        when( module.getLogService() ).thenReturn( NullLogService.getInstance() );
        return module;
    }
}
