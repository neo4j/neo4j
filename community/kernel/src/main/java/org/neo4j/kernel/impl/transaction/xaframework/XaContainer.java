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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.Config;

/**
 * This is a wrapper class containing the logical log, command factory,
 * transaction factory and resource manager. Using the static <CODE>create</CODE>
 * method, a {@link XaResourceManager} will be created and the
 * {@link XaLogicalLog} will be instantiated (not opened).
 * <p>
 * The purpose of this class is to make it easier to setup all that is needed
 * when using the <CODE>xaframework</CODE>. Classes needs to be instantiated
 * in a certain order and references passed between the. <CODE>XaContainer</CODE>
 * will do that work so you only have to supply the <CODE>logical log file</CODE>,
 * your {@link XaCommandFactory} and {@link XaTransactionFactory}
 * implementations.
 */
public class XaContainer
{
    private XaCommandFactory cf;
    private XaLogicalLog log;
    private XaResourceManager rm;
    private XaTransactionFactory tf;

    /**
     * Creates a XaContainer.
     *
     * @param logicalLog
     *            The logical log file name
     * @param cf
     *            The command factory implementation
     * @param tf
     *            The transaction factory implementation
     * @param config Configuration map or null if no config needed
     */
    public static XaContainer create( XaDataSource dataSource,
            String logicalLog, XaCommandFactory cf, XaTransactionFactory tf,
            List<Pair<TransactionInterceptorProvider, Object>> providers,
            Map<Object, Object> config )
    {
        if ( logicalLog == null || cf == null || tf == null )
        {
            throw new IllegalArgumentException( "Null parameter, "
                + "LogicalLog[" + logicalLog + "] CommandFactory[" + cf
                + "TransactionFactory[" + tf + "]" );
        }
        return new XaContainer( dataSource, logicalLog, cf, tf, providers,
                config );
    }

    private XaContainer( XaDataSource dataSource, String logicalLog,
            XaCommandFactory cf, XaTransactionFactory tf,
            List<Pair<TransactionInterceptorProvider, Object>> providers,
            Map<Object, Object> config )
    {
        this.cf = cf;
        this.tf = tf;

        // OK, this is ugly (although it only happens in test cases... config is never
        // null diring normal circumstances.
        TxIdGenerator txIdFactory = config != null ?
                (TxIdGenerator) config.get( TxIdGenerator.class ) : TxIdGenerator.DEFAULT;
        txIdFactory = txIdFactory != null ? txIdFactory : TxIdGenerator.DEFAULT;

        rm = new XaResourceManager( dataSource, tf, txIdFactory, logicalLog );

        if ( "true".equalsIgnoreCase( (String) config.get( Config.INTERCEPT_DESERIALIZED_TRANSACTIONS ) )
             && providers != null )
        {
            log = new InterceptingXaLogicalLog( logicalLog, rm, cf, tf, config,
                    providers );
        }
        else
        {
            log = new XaLogicalLog( logicalLog, rm, cf, tf, config );
        }
        rm.setLogicalLog( log );
        tf.setLogicalLog( log );
    }

    /**
     * Opens the logical log. If the log doesn't exist a new log will be
     * created. If the log exists it will be scaned and non completed
     * transactions will be recovered.
     * <p>
     * This method is only valid to invoke once after the container has been
     * created. Invoking this method again could cause the logical log to be
     * corrupted.
     */
    public void openLogicalLog() throws IOException
    {
        log.open();
    }

    /**
     * Closes the logical log and nulls out all instances.
     */
    public void close()
    {
        try
        {
            if ( log != null )
            {
                log.close();
            }
        }
        catch ( IOException e )
        {
            System.out.println( "Unable to close logical log" );
            e.printStackTrace();
        }
        log = null;
        rm = null;
        cf = null;
        tf = null;
    }

    public XaCommandFactory getCommandFactory()
    {
        return cf;
    }

    public XaLogicalLog getLogicalLog()
    {
        return log;
    }

    public XaResourceManager getResourceManager()
    {
        return rm;
    }

    public XaTransactionFactory getTransactionFactory()
    {
        return tf;
    }
}