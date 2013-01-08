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
package org.neo4j.index.lucene;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexProviders;
import org.neo4j.index.impl.lucene.ConnectionBroker;
import org.neo4j.index.impl.lucene.LuceneDataSource;
import org.neo4j.index.impl.lucene.LuceneIndexImplementation;
import org.neo4j.index.impl.lucene.LuceneXaConnection;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.IndexConnectionBroker;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.index.ReadOnlyIndexConnectionBroker;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.Logging;

public class LuceneKernelExtension extends LifecycleAdapter
{
    private final Config config;
    private final GraphDatabaseService gdb;
    private final TransactionManager txManager;
    private final IndexStore indexStore;
    private final XaFactory xaFactory;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final XaDataSourceManager xaDataSourceManager;
    private final IndexProviders indexProviders;
    private final Logging logging;
    private final TxIdGenerator txIdGenerator;


    public static abstract class Configuration
    {
        public static final GraphDatabaseSetting.BooleanSetting read_only = GraphDatabaseSettings.read_only;
    }

    public LuceneKernelExtension( Config config, GraphDatabaseService gdb, TransactionManager txManager,
                                  IndexStore indexStore, XaFactory xaFactory,
                                  FileSystemAbstraction fileSystemAbstraction,
                                  XaDataSourceManager xaDataSourceManager, IndexProviders indexProviders,
                                  TxIdGenerator txIdGenerator, Logging logging )
    {
        this.config = config;
        this.gdb = gdb;
        this.txManager = txManager;
        this.indexStore = indexStore;
        this.xaFactory = xaFactory;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.xaDataSourceManager = xaDataSourceManager;
        this.indexProviders = indexProviders;
        this.txIdGenerator = txIdGenerator;
        this.logging = logging;
    }

    @Override
    public void start() throws Throwable
    {
        LuceneDataSource luceneDataSource = new LuceneDataSource( config, indexStore, fileSystemAbstraction,
                xaFactory, txIdGenerator, logging );

        xaDataSourceManager.registerDataSource( luceneDataSource );

        IndexConnectionBroker<LuceneXaConnection> broker = config.get( Configuration.read_only ) ? new
                ReadOnlyIndexConnectionBroker<LuceneXaConnection>( txManager )
                : new ConnectionBroker( txManager, luceneDataSource );

        LuceneIndexImplementation indexImplementation = new LuceneIndexImplementation( gdb, luceneDataSource, broker );
        indexProviders.registerIndexProvider( LuceneIndexImplementation.SERVICE_NAME, indexImplementation );
    }

    @Override
    public void stop() throws Throwable
    {
        xaDataSourceManager.unregisterDataSource( LuceneDataSource.DEFAULT_NAME );

        indexProviders.unregisterIndexProvider( LuceneIndexImplementation.SERVICE_NAME );
    }
}
