/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.transaction.TransactionManager;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexImplementation;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.index.impl.lucene.ConnectionBroker;
import org.neo4j.index.impl.lucene.LuceneDataSource;
import org.neo4j.index.impl.lucene.LuceneIndexImplementation;
import org.neo4j.index.impl.lucene.LuceneXaConnection;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.IndexConnectionBroker;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.index.ReadOnlyIndexConnectionBroker;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;

public class LuceneIndexProvider extends IndexProvider
{
    private static List<WeakReference<LuceneIndexImplementation>> previousProviders = new ArrayList<WeakReference<LuceneIndexImplementation>>();
    
    public static abstract class  Configuration
    {
        public static final GraphDatabaseSetting.BooleanSetting read_only = GraphDatabaseSettings.read_only;
    }

    public LuceneIndexProvider( )
    {
        super( LuceneIndexImplementation.SERVICE_NAME );
    }

    @Override
    public IndexImplementation load( DependencyResolver dependencyResolver)
    {
        Config config = dependencyResolver.resolveDependency(Config.class);
        AbstractGraphDatabase gdb = dependencyResolver.resolveDependency(AbstractGraphDatabase.class);
        TransactionManager txManager = dependencyResolver.resolveDependency(TransactionManager.class);
        IndexStore indexStore = dependencyResolver.resolveDependency(IndexStore.class);
        XaFactory xaFactory = dependencyResolver.resolveDependency(XaFactory.class);
        FileSystemAbstraction fileSystemAbstraction = dependencyResolver.resolveDependency(FileSystemAbstraction.class);
        XaDataSourceManager xaDataSourceManager = dependencyResolver.resolveDependency( XaDataSourceManager.class );

        LuceneDataSource luceneDataSource = new LuceneDataSource(config, indexStore, fileSystemAbstraction, xaFactory);

        xaDataSourceManager.registerDataSource(luceneDataSource);

        IndexConnectionBroker<LuceneXaConnection> broker = config.getBoolean( Configuration.read_only ) ? new ReadOnlyIndexConnectionBroker<LuceneXaConnection>( txManager )
                : new ConnectionBroker( txManager, luceneDataSource );

        // TODO This is a hack to support reload of HA instances. Remove if HA supports start/stop of single instance instead
        for( Iterator<WeakReference<LuceneIndexImplementation>> iterator = previousProviders.iterator(); iterator.hasNext(); )
        {
            WeakReference<LuceneIndexImplementation> previousProvider = iterator.next();
            LuceneIndexImplementation indexImplementation = previousProvider.get();
            if (indexImplementation == null)
                iterator.remove();
            else if ( indexImplementation.matches( gdb ) )
                indexImplementation.reset( luceneDataSource, broker );
        }
        
        LuceneIndexImplementation indexImplementation = new LuceneIndexImplementation( gdb, luceneDataSource, broker );
        previousProviders.add( new WeakReference<LuceneIndexImplementation>( indexImplementation ) );
        return indexImplementation;
    }

}
