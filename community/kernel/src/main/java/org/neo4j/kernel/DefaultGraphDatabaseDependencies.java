/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel;

import java.util.Arrays;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.logging.Logging;

public class DefaultGraphDatabaseDependencies extends GraphDatabaseDependencies
{
    public DefaultGraphDatabaseDependencies()
    {
        this( GraphDatabaseSettings.class );
    }

    public DefaultGraphDatabaseDependencies( Logging logging )
    {
        this( logging, GraphDatabaseSettings.class );
    }

    public DefaultGraphDatabaseDependencies( Class<?>... settingsClasses )
    {
        this( null, settingsClasses );
    }

    public DefaultGraphDatabaseDependencies( Logging logging, Class<?>... settingsClasses )
    {
        super(
                logging,
                Arrays.<Class<?>>asList( settingsClasses ),
                Iterables.<KernelExtensionFactory<?>,KernelExtensionFactory>cast( Service.load( KernelExtensionFactory.class ) ),
                Service.load( CacheProvider.class ),
                Service.load( TransactionInterceptorProvider.class ) );
    }
}
