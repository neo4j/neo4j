/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.api.explicitindex;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.explicitindex.AutoIndexOperations;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;

/**
 * This gets notified whenever there are changes to entities and their properties, and given a runtime-configurable set of rules
 * then automatically triggers writes to two special explicit indexes - eg. it automatically keeps these indexes up to date.
 */
public class InternalAutoIndexing implements AutoIndexing
{
    public static final String NODE_AUTO_INDEX = "node_auto_index";
    public static final String RELATIONSHIP_AUTO_INDEX = "relationship_auto_index";

    private final InternalAutoIndexOperations nodes;
    private final InternalAutoIndexOperations relationships;

    public InternalAutoIndexing( Config config, PropertyKeyTokenHolder propertyKeyLookup )
    {
        this.nodes = new InternalAutoIndexOperations( propertyKeyLookup, InternalAutoIndexOperations.EntityType.NODE );
        this.relationships = new InternalAutoIndexOperations( propertyKeyLookup, InternalAutoIndexOperations.EntityType.RELATIONSHIP );

        this.nodes.enabled( config.get( GraphDatabaseSettings.node_auto_indexing ) );
        this.nodes.replacePropertyKeysToInclude( config.get( GraphDatabaseSettings.node_keys_indexable ) );
        this.relationships.enabled( config.get( GraphDatabaseSettings.relationship_auto_indexing ) );
        this.relationships.replacePropertyKeysToInclude( config.get( GraphDatabaseSettings.relationship_keys_indexable ) );
    }

    @Override
    public AutoIndexOperations nodes()
    {
        return nodes;
    }

    @Override
    public AutoIndexOperations relationships()
    {
        return relationships;
    }
}
