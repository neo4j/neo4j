/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.factory;

import java.util.List;

import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.SimpleStatementLocksFactory;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

public class StatementLocksFactorySelector
{
    private final Locks locks;
    private final Config config;
    private final Log log;

    public StatementLocksFactorySelector( Locks locks, Config config, LogService logService )
    {
        this.locks = locks;
        this.config = config;
        this.log = logService.getInternalLog( getClass() );
    }

    public StatementLocksFactory select()
    {
        StatementLocksFactory statementLocksFactory;

        String serviceName = StatementLocksFactory.class.getSimpleName();
        List<StatementLocksFactory> factories = serviceLoadFactories();
        if ( factories.isEmpty() )
        {
            statementLocksFactory = new SimpleStatementLocksFactory();

            log.info( "No services implementing " + serviceName + " found. " +
                      "Using " + SimpleStatementLocksFactory.class.getSimpleName() );
        }
        else if ( factories.size() == 1 )
        {
            statementLocksFactory = factories.get( 0 );

            log.info( "Found single implementation of " + serviceName +
                      ". Namely " + statementLocksFactory.getClass().getSimpleName() );
        }
        else
        {
            throw new IllegalStateException(
                    "Found more than one implementation of " + serviceName + ": " + factories );
        }

        statementLocksFactory.initialize( locks, config );

        return statementLocksFactory;
    }

    /**
     * Load all available factories via {@link Service}.
     * <b>Visible for testing only.</b>
     *
     * @return list of available factories.
     */
    List<StatementLocksFactory> serviceLoadFactories()
    {
        return Iterables.toList( Service.load( StatementLocksFactory.class ) );
    }
}
