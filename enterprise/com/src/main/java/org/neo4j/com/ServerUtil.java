/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.com;

import java.net.InetSocketAddress;
import java.net.URI;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;

import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.first;

public class ServerUtil
{
    private LogicalTransactionStore txs;
    private NeoStoreXaDataSource ds;
    private TransactionIdStore txIdStore;
    private TransactionMetadataCache logPositionCache;
    private TransactionRepresentationCommitProcess commitProcess;

    public static final Predicate<Long> ALL = new Predicate<Long>()
    {
        @Override
        public boolean accept( Long item )
        {
            return true;
        }
    };

    public interface TxHandler
    {
        void accept( Pair<Long, TransactionRepresentation> tx, NeoStoreXaDataSource dataSource );

        void done();
    }

    public static final TxHandler NO_ACTION = new TxHandler()
    {
        @Override
        public void accept( Pair<Long, TransactionRepresentation> tx, NeoStoreXaDataSource dataSource )
        {   // Do nothing
        }

        @Override
        public void done()
        {   // Do nothing
        }
    };

    public static URI getUriForScheme( final String scheme, Iterable<URI> uris )
    {
        return first( filter( new Predicate<URI>()
        {
            @Override
            public boolean accept( URI item )
            {
                return item.getScheme().equals( scheme );
            }
        }, uris ) );
    }

    /**
     * Figure out the host string of a given socket address, similar to the Java 7 InetSocketAddress.getHostString().
     *
     * Calls to this should be replace once Neo4j is Java 7 only.
     *
     * @param socketAddress
     * @return
     */
    public static String getHostString(InetSocketAddress socketAddress )
    {
        if (socketAddress.isUnresolved())
        {
            return socketAddress.getHostName();
        }
        else
        {
            return socketAddress.getAddress().getHostAddress();
        }
    }
}
