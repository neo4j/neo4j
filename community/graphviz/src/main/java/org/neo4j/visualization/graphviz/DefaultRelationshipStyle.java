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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.visualization.PropertyType;

class DefaultRelationshipStyle implements RelationshipStyle
{
    private final DefaultStyleConfiguration config;

    DefaultRelationshipStyle( DefaultStyleConfiguration configuration )
    {
        this.config = configuration;
    }

    public void emitRelationshipStart( Appendable stream, Relationship relationship )
            throws IOException
    {
        Node start = relationship.getStartNode(), end = relationship.getEndNode();
        boolean reversed = config.reverseOrder( relationship );
        long startId = start.getId(), endId = end.getId();
        if ( reversed )
        {
            long tmp = startId;
            startId = endId;
            endId = tmp;
        }
        stream.append( "  N" + startId + " -> N" + endId + " [\n" );
        config.emit( relationship, stream );
        if ( reversed ) stream.append( "    dir = back\n" );
        if ( config.displayRelationshipLabel )
        {
            stream.append( "    label = \"" + config.escapeLabel(config.getTitle( relationship )) + "\\n" );
        }
    }

    public void emitEnd( Appendable stream ) throws IOException
    {
        stream.append( config.displayRelationshipLabel ? "\"\n  ]\n" : "  ]\n" );
    }

    public void emitProperty( Appendable stream, String key, Object value ) throws IOException
    {
        if ( config.displayRelationshipLabel && config.acceptEdgeProperty( key ) )
        {
            PropertyType type = PropertyType.getTypeOf( value );
            config.emitRelationshipProperty( stream, key, type, value );
        }
    }
}
