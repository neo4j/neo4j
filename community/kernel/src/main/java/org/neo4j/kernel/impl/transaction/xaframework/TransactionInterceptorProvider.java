/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.util.List;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Service;

/**
 * The basic service implementation for TransactionInterceptorProviders.
 * Offers two ways to instantiate a TransactionInterceptor - one is
 * standalone, the other is with an existing one as the next in the chain
 * of responsibility.
 */
public abstract class TransactionInterceptorProvider extends Service
{
    public TransactionInterceptorProvider( String name )
    {
        super( name );
    }

    /**
     * Returns the name of this provider
     *
     * @return The name of this provider
     */
    public abstract String name();

    /**
     * Creates a TransactionInterceptor with the given datasource and options.
     * It is possible for this method to return null, signifying that the
     * options passed did not allow for instantiation.
     *
     * @param ds The datasource the TransactionInterceptor will communicate with
     * @param options An object that can be the options to instantiate the
     *            interceptor with - e.g "false" to prevent instantiation
     * @return An implementation of TransactionInterceptor or null if the
     *         options say so.
     */
    public abstract TransactionInterceptor create( XaDataSource ds,
            Object options );

    /**
     * Creates a TransactionInterceptor with the given datasource and options
     * and the given TransactionInterceptor as the next in the chain.
     * It is possible for this method to return null, signifying that the
     * options passed did not allow for instantiation.
     *
     * @param ds The datasource the TransactionInterceptor will communicate with
     * @param options An object that can be the options to instantiate the
     *            interceptor with - e.g "false" to prevent instantiation
     * @param next The next interceptor in the chain - can be null
     * @return An implementation of TransactionInterceptor or null if the
     *         options say so.
     */
    public abstract TransactionInterceptor create( TransactionInterceptor next,
            XaDataSource ds, Object options );

    /**
     * A utility method that given some TransactionInterceptorProviders and
     * their configuration objects returns a fully resolved chain of
     * TransactionInterceptors - the return object is the first interceptor
     * in the chain.
     *
     * @param providers A list of {@link Pair} of
     *            TransactionInterceptorProviders with
     *            the detected config objects
     * @param ds The datasource to instantiate the TransactionInterceptors with
     * @return The fist interceptor in the chain, possibly null
     */
    public static TransactionInterceptor resolveChain(
            List<Pair<TransactionInterceptorProvider, Object>> providers,
            XaDataSource ds )
    {
        TransactionInterceptor first = null;
        for ( Pair<TransactionInterceptorProvider, Object> providerAndConfig : providers )
        {
            TransactionInterceptorProvider provider = providerAndConfig.first();
            Object config = providerAndConfig.other();
            if ( first == null )
            {
                first = provider.create( ds, config );
            }
            else
            {
                TransactionInterceptor temp = provider.create( first, ds,
                        config );
                if ( temp != null )
                {
                    first = temp;
                }
            }
        }
        return first;
    }
}
