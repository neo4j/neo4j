/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.ClosableIterable;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * <CODE>XaDataSource</CODE> is as a factory for creating
 * {@link XaConnection XaConnections}.
 * <p>
 * If you're writing a data source towards a XA compatible resource the
 * implementation should be fairly straight forward. This will basically be a
 * factory for your {@link XaConnection XaConnections} that in turn will wrap
 * the XA compatible <CODE>XAResource</CODE>.
 * <p>
 * If you're writing a data source towards a non XA compatible resource use the
 * {@link XaContainer} and extend the {@link XaConnectionHelpImpl} as your
 * {@link XaConnection} implementation. Here is an example:
 * <p>
 *
 * <pre>
 * <CODE>
 * public class MyDataSource implements XaDataSource {
 *     MyDataSource( params ) {
 *         // ... initalization stuff
 *         container = XaContainer.create( myLogicalLogFile, myCommandFactory,
 *             myTransactionFactory );
 *         // ... more initialization stuff
 *         container.openLogicalLog();
 *     }
 *     public XaConnection getXaCo nnection() {
 *         return new MyXaConnection( params );
 *     }
 *     public void close() {
 *         // ... cleanup
 *         container.close();
 *     }
 * }
 * </CODE>
 * </pre>
 */
public abstract class XaDataSource implements Lifecycle
{
    private byte[] branchId = null;
    private String name = null;

    /**
     * Constructor used by the Neo4j kernel to create datasources.
     *
     */
    public XaDataSource(byte branchId[], String name)
    {
        this.branchId = branchId;
        this.name = name;
    }

    /**
     * Creates a XA connection to the resource this data source represents.
     *
     * @return A connection to an XA resource
     */
    public abstract XaConnection getXaConnection();

    /**
     * Returns any assigned or default branch id for this data source.
     *
     * @return the branch id
     */
    public byte[] getBranchId()
    {
        return this.branchId;
    }

    /**
     * Returns a timestamp when this data source was created. Note this is not
     * the timestamp for the creation of the data source object instance, if
     * the data source is for example a database timestamp is meant to be when
     * the database was created.
     * <p>
     * Creation time together with random identifier can be used to uniqley
     * identify a data source (since it is possible to have multiple sources
     * of same type).
     *
     * @return timestamp when this datasource was created
     */
    public long getCreationTime()
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }

    public File getFileName( long version )
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }

    /**
     * Returns a random identifier that gets generated when the data source is
     * created. Note with "created" we mean first time data source is created
     * and not object creation.
     * <p>
     * Creation time together with the random identifier can be used to uniquely
     * identify a data source (since it is possible to have multiple sources of
     * the same type).
     *
     * @return random identifier for this data source
     */
    public long getRandomIdentifier()
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }

    /**
     * Returns the current version of this data source. A invoke to the
     * {@link #rotateLogicalLog()} when keepLogicalLogs(boolean) is
     * set to <code>true</code> will result in a log with that version created.
     *
     * @return the current version of the logical log
     */
    public long getCurrentLogVersion()
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }

    /**
     * Rotates this logical log. Active transactions get copied to a new logical log.
     *
     * @return the last transaction id of the produced logical log.
     * @throws IOException if unable to read old log or write to new one
     */
    public long rotateLogicalLog() throws IOException
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }

    /**
     * Returns a readable byte channel of the specified logical log.
     *
     * @param version version of the logical log
     * @return readable byte channel of the logical log
     * @throws IOException if no such log exist
     */
    public ReadableByteChannel getLogicalLog( long version ) throws IOException
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }

    /**
     * Tests if a specific logical log exists.
     *
     * @param version the version of the logical log
     * @return <CODE>true</CODE> if the log exists
     */
    public boolean hasLogicalLog( long version )
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }

    public long getLogicalLogLength( long version )
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }

    /**
     * Deletes a log specific logical log.
     *
     * @param version version of log to delete
     *
     * @return true if the log existed and was deleted
     */
    public boolean deleteLogicalLog( long version )
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }

    /**
     * Returns the assigned name of this resource.
     *
     * @return the assigned name of this resource
     */
    public String getName()
    {
        return name;
    }

    /**
     * Turns off/on auto rotate of logical logs. Default is <CODE>true</CODE>.
     *
     * @param rotate <CODE>true</CODE> to turn on
     * @deprecated in favor of {@link GraphDatabaseSettings#logical_log_rotation_threshold}
     */
    @Deprecated
    public void setAutoRotate( boolean rotate )
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }

    /**
     * Sets the target size of the logical log that will cause a rotation of
     * the log if {@link #setAutoRotate(boolean)} is set to <CODE>true</CODE>.
     *
     * @param size target size in bytes
     * @deprecated in favor of setting {@link GraphDatabaseSettings#logical_log_rotation_threshold} to {@code 0}.
     */
    @Deprecated
    public void setLogicalLogTargetSize( long size )
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }

    public ReadableByteChannel getPreparedTransaction( int identifier ) throws IOException
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }

    public void getPreparedTransaction( int identifier, LogBuffer targetBuffer ) throws IOException
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }

    public void applyCommittedTransaction( long txId, ReadableByteChannel transaction ) throws IOException
    {
        getXaContainer().getResourceManager().applyCommittedTransaction( transaction, txId );
    }

    public long applyPreparedTransaction( ReadableByteChannel transaction ) throws IOException
    {
        return getXaContainer().getResourceManager().applyPreparedTransaction( transaction );
    }

    public long getLastCommittedTxId()
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }
    
    public void setLastCommittedTxId( long txId )
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }

    public XaContainer getXaContainer()
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }

    public Pair<Integer,Long> getMasterForCommittedTx( long txId ) throws IOException
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }

    public ClosableIterable<File> listStoreFiles( boolean includeLogicalLogs ) throws IOException
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }

    /**
     * Returns previous value
     */
    public boolean setRecovered( boolean recovered )
    {
        return false;
    }

    public LogExtractor getLogExtractor( long startTxId, long endTxIdHint ) throws IOException
    {
        throw new UnsupportedOperationException( getClass().getName() );
    }
}
