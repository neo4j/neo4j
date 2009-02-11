/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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
package org.neo4j.impl.persistence;

import javax.transaction.TransactionManager;

/**
 * 
 * This class represents the persistence module. It receives lifecycle events
 * from the module framework.
 * 
 */
public class PersistenceModule
{
    private static final String MODULE_NAME = "PersistenceModule";

    private final PersistenceManager persistenceManager;

    private PersistenceSource source;
    private final ResourceBroker broker;

    public PersistenceModule( TransactionManager transactionManager )
    {
        persistenceManager = new PersistenceManager( transactionManager );
        broker = persistenceManager.getResourceBroker();
    }

    public synchronized void init()
    {
        // Do nothing
    }

    public synchronized void start( PersistenceSource persistenceSource )
    {
        this.source = persistenceSource;
        broker.getDispatcher().persistenceSourceAdded( source );
    }

    public synchronized void reload()
    {
        this.stop();
        broker.getDispatcher().persistenceSourceRemoved( source );
    }

    public synchronized void stop()
    {
    }

    public synchronized void destroy()
    {
        // Do nothing
    }

    public String getModuleName()
    {
        return MODULE_NAME;
    }

    public PersistenceManager getPersistenceManager()
    {
        return persistenceManager;
    }
    
    public PersistenceSource getPersistenceSource()
    {
        return source;
    }
}