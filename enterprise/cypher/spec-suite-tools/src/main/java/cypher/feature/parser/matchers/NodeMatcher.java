/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
