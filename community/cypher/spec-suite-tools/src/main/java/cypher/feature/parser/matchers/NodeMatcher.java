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
package cypher.feature.parser.matchers;

import java.util.Set;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

public class NodeMatcher implements ValueMatcher
{
    private final Set<String> labelNames;
    private final MapMatcher propertyMatcher;

    public NodeMatcher( Set<String> labelNames, MapMatcher properties )
    {
        this.labelNames = labelNames;
        this.propertyMatcher = properties;
    }

    @Override
    public boolean matches( Object value )
    {
        if ( value instanceof Node )
        {
            Node node = (Node) value;
            int nbrOfLabels = 0;
            for ( Label l : node.getLabels() )
            {
                if ( !labelNames.contains( l.name() ) )
                {
                    return false;
                }
                ++nbrOfLabels;
            }

            return labelNames.size() == nbrOfLabels && propertyMatcher.matches( node.getAllProperties() );
        }
        return false;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "(" );
        labelNames.forEach( ( l ) -> sb.append( ":" ).append( l ) );
        sb.append( " " ).append( propertyMatcher ).append( ")" );
        return sb.toString();
    }
}
