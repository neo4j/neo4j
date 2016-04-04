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
package org.neo4j.graphdb.factory.builder;

import java.util.Comparator;
import java.util.List;

import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;

public class PriorityFacadeFactorySelector implements GraphDatabaseFacadeFactorySelector
{

    private Comparator<GraphDatabaseFacadeFactory> priorityComparator = new SelectionComparator();

    @Override
    public GraphDatabaseFacadeFactory select( List<GraphDatabaseFacadeFactory> factories )
    {
        if ( factories.isEmpty() )
        {
            throw new RuntimeException( "Should have at least one factory" );
        }
        if ( factories.size() == 1 )
        {
            return factories.get( 0 );
        }
        else
        {
            factories.sort( priorityComparator );
            return factories.get( 0 );
        }
    }

    private class SelectionComparator implements Comparator<GraphDatabaseFacadeFactory>
    {
        @Override
        public int compare( GraphDatabaseFacadeFactory factory1, GraphDatabaseFacadeFactory factory2 )
        {
            return Integer.compare( factory2.selectionPriority(), factory1.selectionPriority() );
        }
    }
}
