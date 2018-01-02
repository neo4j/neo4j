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
package org.neo4j.kernel.impl.coreapi;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.NodeManager;

/**
 * Implementation of an AutoIndexer for Node primitives. It
 * defines the auto index name, the configuration properties
 * names and a wrapper for adapting read write indexes to read only
 * ones so that they are safe to return.
 */
public class NodeAutoIndexerImpl extends AbstractAutoIndexerImpl<Node>
{
    public static abstract class Configuration
    {
        public static final Setting<Boolean> node_auto_indexing = GraphDatabaseSettings.node_auto_indexing;
        public static final Setting<String> node_keys_indexable = GraphDatabaseSettings.node_keys_indexable;
    }

    static final String NODE_AUTO_INDEX = "node_auto_index";
    private Config config;
    private IndexProvider indexProvider;
    private NodeManager nodeManager;

    public NodeAutoIndexerImpl( Config config, IndexProvider indexProvider, NodeManager nodeManager )
    {
        super();
        this.config = config;
        this.indexProvider = indexProvider;
        this.nodeManager = nodeManager;
    }

    @Override
    public void init()
            throws Throwable
    {
    }

    @Override
    public void start()
    {
        setEnabled( config.get( Configuration.node_auto_indexing ) );
        propertyKeysToInclude.addAll( parseConfigList( config.get( Configuration.node_keys_indexable ) ) );
    }

    @Override
    public void stop()
            throws Throwable
    {
    }

    @Override
    public void shutdown()
            throws Throwable
    {
    }

    @Override
    protected Index<Node> getIndexInternal()
    {
        return indexProvider.getOrCreateNodeIndex(
                NODE_AUTO_INDEX, null );
    }

    @Override
    public void setEnabled( boolean enabled )
    {
        super.setEnabled( enabled );
        if ( enabled )
        {
            nodeManager.addNodePropertyTracker(
                    this );
        }
        else
        {
            nodeManager.removeNodePropertyTracker(
                    this );
        }
    }
}
