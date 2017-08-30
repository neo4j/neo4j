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
package org.neo4j.index.internal.gbptree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.helpers.Service;

/**
 * Responsible for instantiating correct {@link TreeNode} based on a format version.
 */
class TreeNodeSelector
{
    static TreeNodeFactory selectHighestPrioritizedTreeNodeFormat()
    {
        List<TreeNodeFactory> all = asList( Service.load( TreeNodeFactory.class ) );
        Collections.sort( all );
        if ( all.isEmpty() )
        {
            throw new IllegalArgumentException( "No formats found" );
        }
        return all.get( 0 );
    }

    private static List<TreeNodeFactory> asList( Iterable<TreeNodeFactory> load )
    {
        List<TreeNodeFactory> factories = new ArrayList<>();
        for ( TreeNodeFactory factory : load )
        {
            factories.add( factory );
        }
        return factories;
    }

    static TreeNodeFactory selectTreeNodeFormat( byte formatIdentifier, byte formatVersion )
    {
        for ( TreeNodeFactory factory : Service.load( TreeNodeFactory.class ) )
        {
            if ( factory.formatIdentifier() == formatIdentifier && factory.formatVersion() == formatVersion )
            {
                return factory;
            }
        }
        throw new IllegalArgumentException( "Couldn't find format with identifier:" + formatIdentifier + " and version:" + formatVersion );
    }

    // TODO: method for migration too?
}

// TODO: SCENARIOS
// TODO: 1. Create new tree from scratch, in community
// TODO: 2. Create new tree from scratch, in enterprise
// TODO: 3. Open existing tree created in enterprise, in enterprise
// TODO: 4. Open existing tree created in enterprise, in community
// TODO: 5. Open existing tree created in community, in enterprise
// TODO: 6. Open existing tree created in community, in community
