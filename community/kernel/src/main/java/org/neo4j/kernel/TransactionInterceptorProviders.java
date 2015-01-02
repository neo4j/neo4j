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

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptor;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

/**
 * @deprecated This will be moved to internal packages in the next major release.
 */
@Deprecated
public class TransactionInterceptorProviders
{
    private final Iterable<TransactionInterceptorProvider> providers;
    private final DependencyResolver resolver;
    private final Config config;


    public TransactionInterceptorProviders( Iterable<TransactionInterceptorProvider> providers, DependencyResolver
            resolver )
    {
        this.providers = providers;
        this.resolver = resolver;
        config = resolver.resolveDependency( Config.class );
    }

    /**
     * A utility method that given some TransactionInterceptorProviders and
     * their configuration objects returns a fully resolved chain of
     * TransactionInterceptors - the return object is the first interceptor
     * in the chain.
     *
     * @param ds The datasource to instantiate the TransactionInterceptors with
     * @return The fist interceptor in the chain, possibly null
     */
    public TransactionInterceptor resolveChain( XaDataSource ds )
    {
        TransactionInterceptor first = null;
        for ( TransactionInterceptorProvider provider : providers )
        {
            String prov = getConfigForInterceptor( provider );
            if ( first == null )
            {
                first = provider.create( ds, prov, resolver );
            }
            else
            {
                TransactionInterceptor temp = provider.create( first, ds,
                        prov, resolver );
                if ( temp != null )
                {
                    first = temp;
                }
            }
        }
        return first;
    }

    public boolean shouldInterceptCommitting()
    {
        return config.get( GraphDatabaseSettings.intercept_committing_transactions ) && providers.iterator().hasNext();
    }

    public boolean shouldInterceptDeserialized()
    {
        return config.get( GraphDatabaseSettings.intercept_deserialized_transactions ) && providers.iterator().hasNext();
    }
    
    public boolean hasAnyInterceptorConfigured()
    {
        for ( TransactionInterceptorProvider provider : providers )
            if ( getConfigForInterceptor( provider ) != null )
                return true;
        return false;
    }

    private String getConfigForInterceptor( TransactionInterceptorProvider provider )
    {
        String prov = config.getParams().get(
                TransactionInterceptorProvider.class.getSimpleName() + "." + provider.name() );
        return prov;
    }
}
