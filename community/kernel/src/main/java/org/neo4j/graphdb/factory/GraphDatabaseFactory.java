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

package org.neo4j.graphdb.factory;

import java.util.List;
import java.util.Map;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.index.IndexIterable;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.EmbeddedReadOnlyGraphDatabase;
import org.neo4j.kernel.KernelExtension;

import static org.neo4j.graphdb.factory.GraphDatabaseSetting.BooleanSetting.*;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.*;

/**
 * Creates a {@link org.neo4j.graphdb.GraphDatabaseService}.
 */
public class GraphDatabaseFactory
{
    protected List<IndexProvider> indexProviders;
    protected List<KernelExtension> kernelExtensions;

    public GraphDatabaseFactory()
    {
        indexProviders = Iterables.toList(Service.load( IndexProvider.class ));
        kernelExtensions = Iterables.toList( Service.load(KernelExtension.class) );
    }

    public GraphDatabaseService newEmbeddedDatabase(String path)
    {
        return newEmbeddedDatabaseBuilder( path ).newGraphDatabase();
    }
    
    public GraphDatabaseBuilder newEmbeddedDatabaseBuilder(final String path)
    {
        return new GraphDatabaseBuilder(new GraphDatabaseBuilder.DatabaseCreator()
        {
            public GraphDatabaseService newDatabase(Map<String, String> config)
            {
                config.put( "ephemeral", "false" );

                if ( TRUE.equalsIgnoreCase(config.get( read_only.name() )))
                    return new EmbeddedReadOnlyGraphDatabase(path, config, indexProviders, kernelExtensions);
                else
                    return new EmbeddedGraphDatabase(path, config, indexProviders, kernelExtensions);
            }
        });
    }
    
    public Iterable<IndexProvider> getIndexProviders()
    {
        return indexProviders;
    }

    /**
     * Sets an {@link org.neo4j.graphdb.index.IndexProvider} iterable source.
     * {@link org.neo4j.kernel.ListIndexIterable} is a flexible provider that works well with
     * dependency injection.
     * @param indexIterable It's actually Iterable<IndexProvider>, but internally typecasted
     *     to workaround bug https://issues.apache.org/jira/browse/ARIES-834 .
     */
    public void setIndexProviders(IndexIterable indexIterable) 
    {
        indexProviders.clear();
        for (IndexProvider indexProvider : indexIterable)
        {
            this.indexProviders.add(indexProvider);
        }
    }
    
    public Iterable<KernelExtension> getKernelExtension()
    {
        return kernelExtensions;
    }
    
    public void setKernelExtensions(Iterable<KernelExtension> newKernelExtensions)
    {
        kernelExtensions.clear();
        for( KernelExtension newKernelExtension : newKernelExtensions )
        {
            kernelExtensions.add( newKernelExtension );
        }
    }
}
