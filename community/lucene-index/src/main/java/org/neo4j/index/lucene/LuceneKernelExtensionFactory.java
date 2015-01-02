/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.graphdb.index.IndexProviders;
import org.neo4j.index.impl.lucene.LuceneIndexImplementation;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class LuceneKernelExtensionFactory extends KernelExtensionFactory<LuceneKernelExtensionFactory.Dependencies>
{
    public interface Dependencies
    {
        Config getConfig();

        GraphDatabaseService getDatabase();

        TransactionManager getTxManager();

        XaFactory getXaFactory();

        FileSystemAbstraction getFileSystem();

        XaDataSourceManager getXaDataSourceManager();

        IndexProviders getIndexProviders();

        IndexStore getIndexStore();
    }

    public LuceneKernelExtensionFactory()
    {
        super( LuceneIndexImplementation.SERVICE_NAME );
    }

    @Override
    public Lifecycle newKernelExtension( Dependencies dependencies ) throws Throwable
    {
        return new LuceneKernelExtension( dependencies.getConfig(), dependencies.getDatabase(),
                dependencies.getTxManager(), dependencies.getIndexStore(), dependencies.getXaFactory(),
                dependencies.getFileSystem(),
                dependencies.getXaDataSourceManager(), dependencies.getIndexProviders() );
    }
}
