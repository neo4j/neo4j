/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.database;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.neo4j.function.Function;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.Logging;

/**
 * A service that provides access to multiple neo4j databases, and allows mapping them to keys.
 */
public class DatabaseRegistry implements Lifecycle
{
    public interface Visitor
    {
        void visit( Database db );
    }

    private final ConcurrentMap<String, Database.Factory> providers = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, DatabaseRegistryEntry> databases = new ConcurrentHashMap<>();
    private final Function<Config, Logging> loggingProvider;
    private final LifeSupport life = new LifeSupport();

    public DatabaseRegistry( Function<Config, Logging> loggingProvider )
    {
        this.loggingProvider = loggingProvider;
    }

    /** Visit a database, acquiring a shared lock on it that keeps it from being dropped. */
    public void visit( String dbKey,  Visitor visitor )
    {
        databases.get(dbKey).visit( visitor );
    }

    public void drop( String dbKey )
    {
        DatabaseRegistryEntry entry = databases.remove( dbKey );
        life.remove( entry );
    }

    public void create( DatabaseDefinition db ) throws NoSuchDatabaseProviderException
    {
        if(!providers.containsKey( db.provider() ))
        {
            throw new NoSuchDatabaseProviderException(db.provider());
        }

        DatabaseRegistryEntry entry = new DatabaseRegistryEntry(providers.get( db.provider() ).newDatabase(
                db.config(), loggingProvider.apply( db.config() ) ) );
        DatabaseRegistryEntry prevEntry = databases.putIfAbsent( db.key(), entry );

        if(prevEntry == null)
        {
            life.add( entry );
        }
    }

    public void addProvider( String providerKey, Database.Factory factory )
    {
        providers.put( providerKey, factory );
    }

    public boolean contains( String dbKey )
    {
        return databases.containsKey( dbKey );
    }

    @Override
    public void init()
    {
        life.init();
    }

    @Override
    public void start()
    {
        life.start();
    }

    @Override
    public void stop() throws Throwable
    {
        life.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        life.shutdown();
    }
}
