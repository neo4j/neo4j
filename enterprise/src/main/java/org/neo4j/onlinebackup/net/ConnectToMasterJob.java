/*
 * Copyright (c) 2009-2010 "Neo Technology,"
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
package org.neo4j.onlinebackup.net;

import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.onlinebackup.ha.AbstractSlave;

public class ConnectToMasterJob extends ConnectionJob
{
    private static enum Status implements JobStatus
    {
        SETUP_GREETING,
        SEND_GREETING,
        GET_RESPONSE,
    }
    
    private final AbstractSlave slave;
    private final String xaDsName;
    private final XaDataSource xaDs;
    
    private long masterVersion;
    private int retries = 0;
    
    public ConnectToMasterJob( Connection connection, AbstractSlave slave, 
            String xaDsName, XaDataSource xaDs )
    {
        super( connection, slave );
        this.slave = slave;
        this.xaDsName = xaDsName;
        this.xaDs = xaDs;
        setStatus( Status.SETUP_GREETING );
    }
    
    private boolean setupGreeting()
    {
        if ( retries > 20 )
        {
            close();
        }
        if ( !acquireWriteBuffer() )
        {
            retries++;
            return false;
        }
        buffer.put( HeaderConstants.SLAVE_GREETING );
        buffer.putLong( xaDs.getRandomIdentifier() );
        buffer.putLong( xaDs.getCreationTime() );
        buffer.putLong( xaDs.getCurrentLogVersion() );
        byte[] bytes = xaDsName.getBytes();
        buffer.putInt( bytes.length );
        buffer.put( bytes );
        buffer.flip();
        log( "Setup greeting" );
        setStatus( Status.SEND_GREETING );
        retries = 0;
        return true;
    }
    
    private boolean sendGreeting()
    {
        if ( retries > 20 )
        {
            close();
        }
        log( "Send greeting" );
        connection.write();
        if ( !buffer.hasRemaining() )
        {
            releaseWriteBuffer();
            setStatus( Status.GET_RESPONSE );
            return true;
        }
        retries++;
        return false;
    }
    
    private boolean getResponse()
    {
        if ( retries > 20 )
        {
            close();
        }
        if ( !acquireReadBuffer() )
        {
            retries++;
            return false;
        }
        try
        {
            // HEADER(1) + DB_VERISON(8)
            buffer.limit( 9 );
            int read = connection.read();
            log( "Get greeting response" );
            if ( read == 1 || read == 9 )
            {
                buffer.flip();
                byte masterGreeting = buffer.get();
                if ( masterGreeting == HeaderConstants.BYE )
                {
                    log( "Problem connecting to master " + connection + 
                    ". Got BYE." );
                    close();
                    return true;
                }
                else if ( masterGreeting != HeaderConstants.MASTER_GREETING )
                {
                    log( "Got unkown greeting[" + masterGreeting + "] from " +  
                        connection );
                    close();
                }
                else if ( read != 9 )
                {
                    retries++;
                    connection.pushBackAllReadData();
                    return false;
                }
                masterVersion = buffer.getLong();
                log( "Got master version: " + masterVersion );
                if ( masterVersion < xaDs.getCurrentLogVersion() )
                {
                    log( "Got wrong version [" + masterVersion + "]" );
                    close();
                    return true;
                }
                setNoRequeue();
                setChainJob( new HandleMasterConnection( connection, slave, 
                    masterVersion, xaDs ) );
                return true;
            }
            else
            {
                retries++;
                if ( read > 0 )
                {
                    connection.pushBackAllReadData();
                }
                return false;
            }
        }
        finally
        {
            releaseReadBuffer();
        }
    }
    
    @Override
    public boolean performJob()
    {
        switch ( (Status) getStatus() )
        {
            case SETUP_GREETING: return setupGreeting();
            case SEND_GREETING: return sendGreeting();
            case GET_RESPONSE: return getResponse();
            default:
                throw new IllegalStateException( "Unkown status: " + 
                    getStatus() );
        }
    }

    @Override
    void connectionClosed()
    {
        System.out.println( "Connection closed " + connection );
    }
    
    public long getMasterVersion()
    {
        return masterVersion;
    }
}