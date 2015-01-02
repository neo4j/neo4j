/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import java.nio.channels.ReadableByteChannel;

import org.neo4j.com.Response;
import org.neo4j.com.ServerUtil;
import org.neo4j.com.TxExtractor;
import org.neo4j.helpers.Triplet;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;

public class HaXaDataSourceManager extends XaDataSourceManager
{
    public HaXaDataSourceManager( StringLogger msgLog )
    {
        super( msgLog );
    }

    public <T> T applyTransactions( Response<T> response )
    {
        return applyTransactions( response, ServerUtil.NO_ACTION );
    }

    public <T> T applyTransactions( Response<T> response, ServerUtil.TxHandler txHandler )
    {
        try
        {
            for ( Triplet<String, Long, TxExtractor> tx : IteratorUtil.asIterable( response.transactions() ) )
            {
                String resourceName = tx.first();
                XaDataSource dataSource = getXaDataSource( resourceName );
                txHandler.accept( tx, dataSource );
                ReadableByteChannel txStream = tx.third().extract();
                try
                {
                    dataSource.applyCommittedTransaction( tx.second(), txStream );
                }
                finally
                {
                    txStream.close();
                }
            }
            txHandler.done();
        }
        catch (Exception e)
        {
            throw new RuntimeException( e );
        }
        finally
        {
            response.close();
        }
        return response.response();
    }
}
