/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.kernel.ha.comm;

import java.io.IOException;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class TransactionApplier extends SimpleChannelUpstreamHandler
{
    private volatile XaDataSourceManager dataSourceManager;

    public void setDataSourceManager( XaDataSourceManager dataSourceManager )
    {
        this.dataSourceManager = dataSourceManager;
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception
    {
        Object message = e.getMessage();
        if ( message instanceof TransactionEntry )
        {
            apply( (TransactionEntry) message );
        }
        else
        {
            ctx.sendUpstream( e );
        }
    }

    private void apply( TransactionEntry transaction ) throws IOException
    {
        XaDataSource dataSource = dataSourceManager.getXaDataSource( transaction.resource );
        dataSource.applyCommittedTransaction( transaction.txId, transaction.data );
    }
}
