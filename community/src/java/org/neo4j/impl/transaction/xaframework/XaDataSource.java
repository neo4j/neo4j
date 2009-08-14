/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.transaction.xaframework;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;

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
public abstract class XaDataSource
{
    private byte[] branchId = null;
    private String name = null;
    
    /**
     * Constructor used by the Neo to create datasources.
     * 
     * @param params
     *            A map containing configuration parameters
     */
    public XaDataSource( Map<?,?> params ) throws InstantiationException
    {
    }

    /**
     * Creates a XA connection to the resource this data source represents.
     * 
     * @return A connection to an XA resource
     */
    public abstract XaConnection getXaConnection();

    /**
     * Closes this data source. Calling <CODE>getXaConnection</CODE> after
     * this method has been invoked is illegal.
     */
    public abstract void close();

    /**
     * Used by the container/transaction manager in place to assign a branch 
     * id for this data source.
     * 
     * @param branchId the branch id
     */
    public void setBranchId( byte branchId[] )
    {
        this.branchId = branchId;
    }

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
        throw new UnsupportedOperationException();
    }
    
    /**
     * Returns a random identifier that gets generated when the data source is 
     * created. Note with "created" we mean first time data source is created 
     * and not object creatoin.
     * <p>
     * Creation time together with random identifier can be used to uniqley 
     * identify a data source (since it is possible to have multiple sources 
     * of same type).
     *  
     * @return random identifier for this data source
     */
    public long getRandomIdentifier()
    {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Returns the current version of this data source. A invoke to the 
     * {@link #rotateLogicalLog()} when {@link #keepLogicalLogs(boolean)} is 
     * set to <code>true</code> will result in a log with that version created.
     * 
     * @return the current version of the logical log
     */
    public long getCurrentLogVersion()
    {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Attempts to apply a logical log to this data source.
     * 
     * @param byteChannel readable channel containing the logical log data
     * 
     * @throws IOException if a problem with reading the log occurs
     * @throws IllegalStateException if log being applied is not of right 
     * version, if not in backup slave mode or there are active transactions
     */
    public void applyLog( ReadableByteChannel byteChannel ) throws IOException
    {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Rotates this logical log. If {@link #keepLogicalLogs(boolean)} is 
     * configured to true the log will be saved and can be retrieved with the 
     * {@link #getLogicalLog(long)} method. If not it will be deleted. Active 
     * transactions get copied to a new logical log.
     * 
     * @throws IOException if unable to read old log or write to new one
     */
    public void rotateLogicalLog() throws IOException
    {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }
    
    /**
     * Tests if a specific logical log exists.
     * 
     * @param version the version of the logical log
     * @return <CODE>true</CODE> if the log exists
     */
    public boolean hasLogicalLog( long version )
    {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

    /**
     * Sets wether logical logs should be saved upon rotation or not. Default 
     * is <CODE>false</CODE>.
     * 
     * @param keepLogs <CODE>true</CODE> means save, <CODE>false</CODE> means 
     * delete
     */
    public void keepLogicalLogs( boolean keepLogs )
    {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Used by the container to assign a name to this resource. 
     * 
     * @param name name of this resource
     */
    public void setName( String name )
    {
        this.name = name;
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
     * Makes this data source a backup slave. This method can not be called 
     * while there are active transactions. Once set in "backup slave" mode 
     * no new transactions can start, the resource has to be closed and 
     * reopened for that.
     *
     * @throws IllegalStateException if this resource has active transactions
     */
    public void makeBackupSlave()
    {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Turns off/on auto rotate of logical logs. Default is <CODE>true</CODE>.
     * 
     * @param rotate <CODE>true</CODE> to turn on
     */
    public void setAutoRotate( boolean rotate )
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Sets the target size of the logical log that will cause a rotation of 
     * the log if {@link #setAutoRotate(boolean)} is set to <CODE>true</CODE>.
     * 
     * @param size target size in bytes
     */
    public void setLogicalLogTargetSize( long size )
    {
        throw new UnsupportedOperationException();
    }
}