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
