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

/**
 * This class represents the {@link IdGenerator} module. It receives lifecycle
 * events from the module framework and supports configuration of the
 * IdGenerator.
 * <P>
 * Generally, configuration of the IdGenerator must be done before the first
 * invocation of the {@link #start} method. This is because the IdGenerator does
 * not currently support the modular start/stop/reload operations and there is
 * no way to guarantee consistent behavior if the IdGenerator is not halted
 * during reconfiguration.
 * <P>
 * @see org.neo4j.impl.persistence.IdGenerator
 */
public class IdGeneratorModule
{
    private static final String MODULE_NAME = "IdGeneratorModule";

    private PersistenceSource persistenceSource = null;
    private final IdGenerator idGenerator;

    public IdGeneratorModule()
    {
        this.idGenerator = new IdGenerator();
    }

    public IdGenerator getIdGenerator()
    {
        return idGenerator;
    }

    public synchronized void init()
    {
        // Do nothing
    }

    public synchronized void start()
    {
        // Configure the IdGenerator
        idGenerator.configure( this.getPersistenceSource() );
    }

    public synchronized void reload()
    {
        throw new RuntimeException( "IdGenerator does not support reload" );
    }

    public synchronized void stop()
    {
        // Do nothing
    }

    public synchronized void destroy()
    {
        // Do nothing
    }

    public String getModuleName()
    {
        return MODULE_NAME;
    }

    private synchronized PersistenceSource getPersistenceSource()
    {
        return this.persistenceSource;
    }

    public synchronized void setPersistenceSourceInstance(
        PersistenceSource source )
    {
        this.persistenceSource = source;
    }

    public String toString()
    {
        return this.getModuleName();
    }
}