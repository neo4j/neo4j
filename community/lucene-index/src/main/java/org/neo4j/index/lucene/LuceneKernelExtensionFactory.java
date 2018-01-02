/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.graphdb.index.IndexProviders;
import org.neo4j.index.impl.lucene.LuceneIndexImplementation;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class LuceneKernelExtensionFactory extends KernelExtensionFactory<LuceneKernelExtensionFactory.Dependencies>
{
    public interface Dependencies
    {
        Config getConfig();

        IndexProviders getIndexProviders();

        IndexConfigStore getIndexStore();
    }

    public LuceneKernelExtensionFactory()
    {
        super( LuceneIndexImplementation.SERVICE_NAME );
    }

    @Override
    public Lifecycle newInstance( KernelContext context, Dependencies dependencies ) throws Throwable
    {
        return new LuceneKernelExtension( context.storeDir(), dependencies.getConfig(), dependencies.getIndexStore(),
                context.fileSystem(), dependencies.getIndexProviders() );
    }
}
