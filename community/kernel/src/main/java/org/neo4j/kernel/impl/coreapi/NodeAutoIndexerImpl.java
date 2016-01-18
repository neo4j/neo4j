/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.Collection;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

/**
 * Implementation of an AutoIndexer for Node primitives. It
 * defines the auto index name, the configuration properties
 * names and a wrapper for adapting read write indexes to read only
 * ones so that they are safe to return.
 */
public class NodeAutoIndexerImpl extends AbstractAutoIndexerImpl<Node>
{
    static final String NODE_AUTO_INDEX = "node_auto_index";
    private final IndexProvider indexProvider;
    private final GraphDatabaseFacade.SPI spi;

    public NodeAutoIndexerImpl( boolean enabled, Collection<String> propertiesToIndex, IndexProvider indexProvider, GraphDatabaseFacade.SPI spi )
    {
        super();
        this.indexProvider = indexProvider;
        this.spi = spi;
        setEnabled( enabled );
        propertyKeysToInclude.addAll( propertiesToIndex );
    }

    @Override
    protected Index<Node> getIndexInternal()
    {
        return indexProvider.getOrCreateNodeIndex( NODE_AUTO_INDEX, null );
    }

    @Override
    public void setEnabled( boolean enabled )
    {
        super.setEnabled( enabled );
        if ( enabled )
        {
            spi.addNodePropertyTracker( this );
        }
        else
        {
            spi.removeNodePropertyTracker( this );
        }
    }
}
