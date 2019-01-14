/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.backup.impl;

import org.neo4j.com.storecopy.StoreCopyClientMonitor;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Monitor for events that should be displayed to neo4j-admin backup stdout
 */
class BackupOutputMonitor implements StoreCopyClientMonitor
{
    private final Log log;

    BackupOutputMonitor( OutsideWorld outsideWorld )
    {
        LogProvider stdOutLogProvider = FormattedLogProvider.toOutputStream( outsideWorld.outStream() );
        log = stdOutLogProvider.getLog( BackupOutputMonitor.class );
    }

    @Override
    public void startReceivingStoreFiles()
    {
        log.info( "Start receiving store files" );
    }

    @Override
    public void finishReceivingStoreFiles()
    {
        log.info( "Finish receiving store files" );
    }

    @Override
    public void startReceivingStoreFile( String file )
    {
        log.info( "Start receiving store file %s", file );
    }

    @Override
    public void finishReceivingStoreFile( String file )
    {
        log.info( "Finish receiving store file %s", file );
    }

    @Override
    public void startReceivingTransactions( long startTxId )
    {
        log.info( "Start receiving transactions from %d", startTxId );
    }

    @Override
    public void finishReceivingTransactions( long endTxId )
    {
        log.info( "Finish receiving transactions at %d", endTxId );
    }

    @Override
    public void startRecoveringStore()
    {
        log.info( "Start recovering store" );
    }

    @Override
    public void finishRecoveringStore()
    {
        log.info( "Finish recovering store" );
    }

    @Override
    public void startReceivingIndexSnapshots()
    {
        log.info( "Start receiving index snapshots" );
    }

    @Override
    public void startReceivingIndexSnapshot( long indexId )
    {
        log.info( "Start receiving index snapshot id %d", indexId );
    }

    @Override
    public void finishReceivingIndexSnapshot( long indexId )
    {
        log.info( "Finished receiving index snapshot id %d", indexId );
    }

    @Override
    public void finishReceivingIndexSnapshots()
    {
        log.info( "Finished receiving index snapshots" );
    }
}
