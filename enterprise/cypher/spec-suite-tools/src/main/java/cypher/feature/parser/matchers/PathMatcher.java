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
            if ( path.length() != pathLinks.size() )
            {
                return false;
            }
            if ( pathLinks.isEmpty() )
            {
                return path.length() == 0
                       && singleNodePath.matches( path.startNode() )
                       && singleNodePath.matches( path.endNode() );
            }
            else
            {
                Iterator<Relationship> relationships = path.relationships().iterator();
                for ( PathLinkMatcher pathLink : pathLinks )
                {
                    Relationship next = relationships.next();
                    if ( !pathLink.matches( next ) )
                    {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString()
    {
        if ( pathLinks.isEmpty() )
        {
            return "<" + singleNodePath + ">";
        }
        else
        {
            StringBuilder sb = new StringBuilder(  );
            sb.append( "<" );
            pathLinks.forEach( sb::append );
            sb.append( ">" );
            return sb.toString();
        }
    }
}
