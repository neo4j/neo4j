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
package cypher.feature.parser.matchers;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

public class PathMatcher implements ValueMatcher
{
    private final NodeMatcher singleNodePath;
    private final List<PathLinkMatcher> pathLinks;

    public PathMatcher( List<PathLinkMatcher> pathLinks )
    {
        this.pathLinks = pathLinks;
        this.singleNodePath = null;
    }

    public PathMatcher( NodeMatcher pathNode )
    {
        this.singleNodePath = pathNode;
        pathLinks = Collections.emptyList();
    }

    @Override
    public boolean matches( Object value )
    {
        if ( value instanceof Path )
        {
            Path path = (Path) value;
            if ( pathLinks.isEmpty() )
            {
                boolean matches = path.length() == 0;
                matches &= singleNodePath.matches( path.startNode() );
                matches &= singleNodePath.matches( path.endNode() );
                return matches;
            }
            else
            {
                boolean matches = path.length() == pathLinks.size();
                if ( matches )
                {
                    Iterator<Relationship> relationships = path.relationships().iterator();
                    for ( PathLinkMatcher pathLink : pathLinks )
                    {
                        pathLink.matches( relationships.next() );
                    }
                }
                return matches;
            }
        }
        return false;
    }

    @Override
    public String toString()
    {
        if ( pathLinks.isEmpty() )
        {
            return "PathMatcher for " + singleNodePath;
        }
        else
        {
            return "PathMatcher for " + pathLinks;
        }
    }
}
