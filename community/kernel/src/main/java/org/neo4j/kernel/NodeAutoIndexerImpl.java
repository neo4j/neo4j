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
package org.neo4j.kernel;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;

/**
 * Implementation of an AutoIndexer for Node primitives. It
 * defines the auto index name, the configuration properties
 * names and a wrapper for adapting read write indexes to read only
 * ones so that they are safe to return.
 */
class NodeAutoIndexerImpl extends AbstractAutoIndexerImpl<Node>
{
    static final String NODE_AUTO_INDEX = "node_auto_index";

    public NodeAutoIndexerImpl( EmbeddedGraphDbImpl gdb )
    {
        super( gdb );
    }

    @Override
    protected String getAutoIndexConfigListName()
    {
        return Config.NODE_KEYS_INDEXABLE;
    }

    @Override
    protected String getAutoIndexName()
    {
        return NODE_AUTO_INDEX;
    }

    @Override
    protected String getEnableConfigName()
    {
        return Config.NODE_AUTO_INDEXING;
    }

    @Override
    protected Index<Node> getIndexInternal()
    {
        return ( (IndexManagerImpl) getGraphDbImpl().index() ).getOrCreateNodeIndex(
                NODE_AUTO_INDEX, null );
    }

    @Override
    public void setEnabled( boolean enabled )
    {
        super.setEnabled( enabled );
        if ( enabled )
        {
            getGraphDbImpl().getConfig().getGraphDbModule().getNodeManager().addNodePropertyTracker(
                this );
        }
        else
        {
            getGraphDbImpl().getConfig().getGraphDbModule().getNodeManager().removeNodePropertyTracker(
                    this );
        }
    }
}
