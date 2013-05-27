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
package org.neo4j.kernel.impl.nioneo.xa;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.List;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.ClosableIterable;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.SchemaCache;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.store.WindowPoolStats;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogExtractor;
import org.neo4j.kernel.impl.transaction.xaframework.XaContainer;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.info.DiagnosticsManager;

public class ShutdownXaDataSource extends NeoStoreXaDataSource
{
    public ShutdownXaDataSource()
    {
        super( new Config(), null, null, null, null, null, null, null, null, null, null );
    }

    @Override
    public long getCreationTime()
    {
        throw databaseIsShutdownError();
    }

    @Override
    public long getCurrentLogVersion()
    {
        throw databaseIsShutdownError();
    }

    @Override
    public long getHighestPossibleIdInUse( Class<?> clazz )
    {
        throw databaseIsShutdownError();
    }

    @Override
    public IndexingService getIndexService()
    {
        throw databaseIsShutdownError();
    }

    @Override
    public long getLastCommittedTxId()
    {
        throw databaseIsShutdownError();
    }

    @Override
    public NeoStore getNeoStore()
    {
        throw databaseIsShutdownError();
    }

    @Override
    public long getNumberOfIdsInUse( Class<?> clazz )
    {
        throw databaseIsShutdownError();
    }

    @Override
    public DefaultSchemaIndexProviderMap getProviderMap()
    {
        throw databaseIsShutdownError();
    }

    @Override
    public long getRandomIdentifier()
    {
        throw databaseIsShutdownError();
    }

    @Override
    public SchemaCache getSchemaCache()
    {
        throw databaseIsShutdownError();
    }

    @Override
    public String getStoreDir()
    {
        throw databaseIsShutdownError();
    }

    @Override
    public StoreId getStoreId()
    {
        throw databaseIsShutdownError();
    }

    @Override
    public List<WindowPoolStats> getWindowPoolStats()
    {
        throw databaseIsShutdownError();
    }

    @Override
    public NeoStoreXaConnection getXaConnection()
    {
        throw databaseIsShutdownError();
    }

    @Override
    public XaContainer getXaContainer()
    {
        throw databaseIsShutdownError();
    }

    @Override
    public long incrementAndGetLogVersion()
    {
        throw databaseIsShutdownError();
    }

    @Override
    public void init()
    {
        throw databaseIsShutdownError();
    }

    @Override
    public boolean isReadOnly()
    {
        throw databaseIsShutdownError();
    }

    @Override
    public ClosableIterable<File> listStoreFiles( boolean includeLogicalLogs )
    {
        throw databaseIsShutdownError();
    }

    @Override
    public long nextId( Class<?> clazz )
    {
        throw databaseIsShutdownError();
    }

    @Override
    public void registerDiagnosticsWith( DiagnosticsManager manager )
    {
        throw databaseIsShutdownError();
    }

    @Override
    public void setLastCommittedTxId( long txId )
    {
        throw databaseIsShutdownError();
    }

    @Override
    public boolean setRecovered( boolean recovered )
    {
        throw databaseIsShutdownError();
    }

    @Override
    public void shutdown()
    {
    }

    @Override
    public void start() throws IOException
    {
        throw databaseIsShutdownError();
    }

    @Override
    public void stop()
    {
        throw databaseIsShutdownError();
    }

    @Override
    public boolean deleteLogicalLog( long version )
    {
        throw databaseIsShutdownError();
    }

    @Override
    public File getFileName( long version )
    {
        throw databaseIsShutdownError();
    }

    @Override
    public LogExtractor getLogExtractor( long startTxId, long endTxIdHint ) throws IOException
    {
        throw databaseIsShutdownError();
    }

    @Override
    public ReadableByteChannel getLogicalLog( long version ) throws IOException
    {
        throw databaseIsShutdownError();
    }

    @Override
    public long getLogicalLogLength( long version )
    {
        throw databaseIsShutdownError();
    }

    @Override
    public Pair<Integer, Long> getMasterForCommittedTx( long txId ) throws IOException
    {
        throw databaseIsShutdownError();
    }

    @Override
    public ReadableByteChannel getPreparedTransaction( int identifier ) throws IOException
    {
        throw databaseIsShutdownError();
    }

    @Override
    public void getPreparedTransaction( int identifier, LogBuffer targetBuffer ) throws IOException
    {
        throw databaseIsShutdownError();
    }

    @Override
    public boolean hasLogicalLog( long version )
    {
        throw databaseIsShutdownError();
    }

    @Override
    public long rotateLogicalLog() throws IOException
    {
        throw databaseIsShutdownError();
    }

    @Override
    public void setAutoRotate( boolean rotate )
    {
        throw databaseIsShutdownError();
    }

    @Override
    protected void setLogicalLogAtCreationTime( XaLogicalLog logicalLog )
    {
        throw databaseIsShutdownError();
    }

    @Override
    public void setLogicalLogTargetSize( long size )
    {
        throw databaseIsShutdownError();
    }

    @Override
    public void applyCommittedTransaction( long txId, ReadableByteChannel transaction ) throws IOException
    {
        throw databaseIsShutdownError();
    }

    @Override
    public long applyPreparedTransaction( ReadableByteChannel transaction ) throws IOException
    {
        throw databaseIsShutdownError();
    }

    @Override
    public byte[] getBranchId()
    {
        throw databaseIsShutdownError();
    }

    @Override
    public String getName()
    {
        throw databaseIsShutdownError();
    }

    private DatabaseShutdownException databaseIsShutdownError()
    {
        return new DatabaseShutdownException( );
    }
}
