/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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

    private PersistenceManager persistenceManager;

    public PersistenceModule()
    {
    }

    public synchronized void init()
    {
        // Do nothing
    }

    public synchronized void start( TransactionManager transactionManager, 
        PersistenceSource persistenceSource )
    {
        this.persistenceManager = new PersistenceManager( transactionManager, 
            persistenceSource );
    }

    public synchronized void reload()
    {
        throw new UnsupportedOperationException();
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
}