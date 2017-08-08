/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.schema.NativeSchemaNumberIndexProvider;
import org.neo4j.kernel.impl.index.schema.NativeSelector;
import org.neo4j.kernel.impl.index.schema.fusion.FusionSchemaIndexProvider;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.logging.LogProvider;

@Service.Implementation( KernelExtensionFactory.class )
public class NativeLuceneFusionSchemaIndexProviderFactory
        extends KernelExtensionFactory<NativeLuceneFusionSchemaIndexProviderFactory.Dependencies>
{
    public static final String KEY = LuceneSchemaIndexProviderFactory.KEY + "+" + NativeSchemaNumberIndexProvider.KEY;

    public static final SchemaIndexProvider.Descriptor DESCRIPTOR = new SchemaIndexProvider.Descriptor( KEY, "0.1" );

    public interface Dependencies extends LuceneSchemaIndexProviderFactory.Dependencies
    {
        PageCache pageCache();

        RecoveryCleanupWorkCollector recoveryCleanupWorkCollector();
    }

    public NativeLuceneFusionSchemaIndexProviderFactory()
    {
        super( KEY );
    }

    @Override
    public FusionSchemaIndexProvider newInstance( KernelContext context, Dependencies dependencies ) throws Throwable
    {
        // create native schema index provider
        boolean readOnly = isReadOnly( dependencies.getConfig(), context.databaseInfo().operationalMode );
        LogProvider logging = dependencies.getLogging().getInternalLogProvider();
        NativeSchemaNumberIndexProvider nativeProvider = new NativeSchemaNumberIndexProvider( dependencies.pageCache(),
                context.storeDir(), logging, dependencies.recoveryCleanupWorkCollector(), readOnly );

        // create lucene schema index provider
        LuceneSchemaIndexProvider luceneProvider = LuceneSchemaIndexProviderFactory.create( context, dependencies );

        return new FusionSchemaIndexProvider( nativeProvider, luceneProvider, new NativeSelector(), DESCRIPTOR, 50 );
    }

    private static boolean isReadOnly( Config config, OperationalMode operationalMode )
    {
        return config.get( GraphDatabaseSettings.read_only ) && (OperationalMode.single == operationalMode);
    }
}
