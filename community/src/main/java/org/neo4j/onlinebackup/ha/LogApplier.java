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
package org.neo4j.onlinebackup.ha;

import java.nio.channels.ReadableByteChannel;

import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class LogApplier extends Thread
{
    private volatile boolean run = true;
    
    private final XaDataSource[] xaDataSources;
    
    LogApplier( XaDataSource[] xaDataSources )
    {
        this.xaDataSources = xaDataSources;
    }
    
    public void run()
    {
        try
        {
            while ( run )
            {
                for ( XaDataSource xaDs : xaDataSources )
                {
                    long logVersion = xaDs.getCurrentLogVersion();
                    if ( xaDs.hasLogicalLog( logVersion ) )
                    {
                        ReadableByteChannel logChannel = 
                            xaDs.getLogicalLog( logVersion );
                        xaDs.applyLog( logChannel );
                    }
                    else
                    {
                        synchronized ( this )
                        {
                            try
                            {
                                this.wait( 250 );
                            }
                            catch ( InterruptedException e )
                            {
                                interrupted();
                            }
                        }
                    }
                }
            }
        }
        catch ( Exception e )
        {
            System.err.println( "Failed to apply log: " + e );
            e.printStackTrace();
        }
        finally
        {
            run = false;
        }
    }
    
    public void stopApplyLogs()
    {
        run = false;
    }
}