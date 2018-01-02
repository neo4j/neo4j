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
package org.neo4j.visualization.graphviz;

import java.io.IOException;
import java.util.Iterator;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

public class SimpleNodeStyle extends DefaultNodeStyle
{
    boolean hasLabels = false;

    SimpleNodeStyle( DefaultStyleConfiguration configuration )
    {
        super( configuration );
    }

    @Override
    public void emitNodeStart( Appendable stream, Node node )
            throws IOException
    {
        stream.append( "  N" + node.getId() + " [\n" );
        config.emit( node, stream );
        stream.append( "    label = \"" );
        Iterator<Label> labels = node.getLabels().iterator();
        hasLabels = labels.hasNext();
        if ( hasLabels )
        {
            hasLabels = labels.hasNext();
            if ( hasLabels )
            {
                stream.append( "{" );
                while ( labels.hasNext() )
                {
                    stream.append( labels.next()
                            .name() );
                    if ( labels.hasNext() )
                    {
                        stream.append( ", " );
                    }
                }
                stream.append( "|" );
            }
        }
    }

    @Override
    public void emitEnd( Appendable stream ) throws IOException
    {
        if ( hasLabels )
        {
            stream.append( "}\"\n  ]\n" );
        }
        else
        {
            stream.append( "\"\n  ]\n" );
        }
    }
}
