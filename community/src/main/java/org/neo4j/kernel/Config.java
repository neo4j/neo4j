/*
 * Copyright (c) 2002-2010 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.cache.AdaptiveCacheManager;
import org.neo4j.kernel.impl.core.LockReleaser;
import org.neo4j.kernel.impl.core.GraphDbModule;
import org.neo4j.kernel.impl.event.EventModule;
import org.neo4j.kernel.impl.persistence.IdGeneratorModule;
import org.neo4j.kernel.impl.persistence.PersistenceModule;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.TxModule;

/**
 * A non-standard configuration object.
 */
public class Config
{
    private EventModule eventModule;
    private AdaptiveCacheManager cacheManager;
    private TxModule txModule;
    private LockManager lockManager;
    private LockReleaser lockReleaser;
    private PersistenceModule persistenceModule;
    private boolean create = false;
    private String persistenceSourceName;
    private IdGeneratorModule idGeneratorModule;
    private GraphDbModule graphDbModule;
    private String storeDir;
    private final Map<Object, Object> params;

    private final boolean readOnly;
    private final boolean backupSlave;
    
    Config( GraphDatabaseService graphDb, String storeDir, Map<Object, Object> params )
    {
        this.storeDir = storeDir;
        this.params = params;
        String readOnlyStr = (String) params.get( "read_only" );
        if ( readOnlyStr != null && readOnlyStr.toLowerCase().equals( "true" ) )
        {
            readOnly = true;
        }
        else
        {
            readOnly = false;
        }
        String backupSlaveStr = (String) params.get( "backup_slave" );
        if ( backupSlaveStr != null && 
                backupSlaveStr.toLowerCase().equals( "true" ) )
        {
            backupSlave = true;
        }
        else
        {
            backupSlave = false;
        }
        params.put( "read_only", readOnly );
        eventModule = new EventModule();
        cacheManager = new AdaptiveCacheManager();
        if ( !readOnly )
        {
            txModule = new TxModule( this.storeDir );
        }
        else
        {
            txModule = new TxModule( true );
        }
        lockManager = new LockManager( txModule.getTxManager() );
        lockReleaser = new LockReleaser( lockManager, txModule.getTxManager() );
        persistenceModule = new PersistenceModule();
        idGeneratorModule = new IdGeneratorModule();
        graphDbModule = new GraphDbModule( graphDb, cacheManager, lockManager,
                txModule.getTxManager(), idGeneratorModule.getIdGenerator(),
                readOnly );
    }

    void setPersistenceSource( String name, boolean create )
    {
        persistenceSourceName = name;
        this.create = create;
    }

    String getPersistenceSource()
    {
        return persistenceSourceName;
    }

    boolean getCreatePersistenceSource()
    {
        return create;
    }

    public EventModule getEventModule()
    {
        return eventModule;
    }

    public TxModule getTxModule()
    {
        return txModule;
    }

    public GraphDbModule getGraphDbModule()
    {
        return graphDbModule;
    }

    public PersistenceModule getPersistenceModule()
    {
        return persistenceModule;
    }

    IdGeneratorModule getIdGeneratorModule()
    {
        return idGeneratorModule;
    }

    public LockManager getLockManager()
    {
        return lockManager;
    }

    public LockReleaser getLockReleaser()
    {
        return lockReleaser;
    }

    public Map<Object, Object> getParams()
    {
        return this.params;
    }

    boolean isReadOnly()
    {
        return readOnly;
    }

    boolean isBackupSlave()
    {
        return backupSlave;
    }
}
