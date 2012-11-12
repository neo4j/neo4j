/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.backup.log;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptor;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;

@Service.Implementation( TransactionInterceptorProvider.class )
public class VerifyingTransactionInterceptorProvider extends
        TransactionInterceptorProvider
{
    public static final String NAME = "verifying";

    public VerifyingTransactionInterceptorProvider()
    {
        super( NAME );
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public VerifyingTransactionInterceptor create( XaDataSource ds,
            Object options, DependencyResolver dependencyResolver )
    {
        if ( !( options instanceof String ) )
        {
            return null;
        }
        String[] config = ((String) options).split( ";" );
        if ( !"true".equalsIgnoreCase( config[0] ) )
        {
            return null;
        }
        Map<String, String> extra = new HashMap<String, String>();
        for ( int i = 1; i < config.length; i++ )
        {
            String[] parts = config[i].split( "=", 2 );
            extra.put( parts[0].toLowerCase(), parts.length == 1 ? "true" : parts[1] );
        }
        return new VerifyingTransactionInterceptor( (NeoStoreXaDataSource) ds, dependencyResolver.resolveDependency( StringLogger.class ),
                VerifyingTransactionInterceptor.CheckerMode.DIFF, true, extra );
    }

    @Override
    public VerifyingTransactionInterceptor create( TransactionInterceptor next,
            XaDataSource ds, Object options, DependencyResolver dependencyResolver )
    {
        VerifyingTransactionInterceptor result = create( ds, options, dependencyResolver );
        result.setNext( next );
        return result;
    }
}
