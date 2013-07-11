/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.consistency.checking.incremental.intercept;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.consistency.checking.incremental.DiffCheck;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptor;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;

abstract class CheckingTransactionInterceptorProvider extends TransactionInterceptorProvider
{
    public CheckingTransactionInterceptorProvider( String name )
    {
        super( name );
    }

    abstract DiffCheck createChecker( String mode, StringLogger logger );

    @Override
    public CheckingTransactionInterceptor create( XaDataSource ds, String options,
                                                  DependencyResolver dependencyResolver )
    {
        if ( !(ds instanceof NeoStoreXaDataSource) || !(options != null) )
        {
            return null;
        }
        String[] config = options.split( ";" );
        String mode = config[0];
        Map<String, String> parameters = new HashMap<>();
        for ( int i = 1; i < config.length; i++ )
        {
            String[] parts = config[i].split( "=" );
            parameters.put( parts[0].toLowerCase(), parts.length == 1 ? "true" : parts[1] );
        }
        StringLogger logger = dependencyResolver.resolveDependency( StringLogger.class );
        DiffCheck check = createChecker( mode, logger );
        if ( check == null )
        {
            return null;
        }
        else
        {
            String log = parameters.get( "log" );
            return new CheckingTransactionInterceptor( check, (NeoStoreXaDataSource) ds );
        }
    }

    @Override
    public CheckingTransactionInterceptor create( TransactionInterceptor next, XaDataSource ds, String options,
                                                  DependencyResolver dependencyResolver )
    {
        CheckingTransactionInterceptor interceptor = create( ds, options, dependencyResolver );
        if ( interceptor != null )
        {
            interceptor.setNext( next );
        }
        return interceptor;
    }
}
