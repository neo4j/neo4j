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
import org.neo4j.visualization.graphviz.StyleParameter.Simple;
import org.neo4j.visualization.graphviz.color.AutoRelationshipTypeColor;

public class AsciiDocStyle extends GraphStyle
{
    static final Simple SIMPLE_PROPERTY_STYLE = StyleParameter.Simple.PROPERTY_AS_KEY_EQUALS_VALUE;
    static final DefaultStyleConfiguration PLAIN_STYLE = new DefaultStyleConfiguration(
            SIMPLE_PROPERTY_STYLE );

    public AsciiDocStyle()
    {
        super();
    }

    AsciiDocStyle( StyleParameter... parameters )
    {
        super( parameters );
    }

    public AsciiDocStyle( NodeStyle nodeStyle, RelationshipStyle edgeStyle )
    {
        super( nodeStyle, edgeStyle );
    }

    @Override
    protected void emitGraphStart( Appendable stream ) throws IOException
    {
    }

    @Override
    protected void emitGraphEnd( Appendable stream ) throws IOException
    {
    }

    public static AsciiDocStyle withAutomaticRelationshipTypeColors()
    {
        return new AsciiDocStyle( new DefaultNodeStyle( PLAIN_STYLE ),
                new DefaultRelationshipStyle( new DefaultStyleConfiguration(
                        AsciiDocStyle.SIMPLE_PROPERTY_STYLE,
                        new AutoRelationshipTypeColor() ) ) );
    }
}
