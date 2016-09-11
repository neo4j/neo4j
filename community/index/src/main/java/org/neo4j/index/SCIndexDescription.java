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
package org.neo4j.index;

import org.neo4j.graphdb.Direction;

public class SCIndexDescription
{
    private String description;
    public final String firstLabel;
    public final String secondLabel;
    public final String relationshipType;
    public final Direction direction;
    public final String relationshipPropertyKey;
    public final String nodePropertyKey;

    public SCIndexDescription()
    {
        this( "", "", "", Direction.BOTH, "", null );
    }

    public SCIndexDescription( String firstLabel,
            String secondLabel,
            String relationshipType,
            Direction direction,
            String relationshipPropertyKey,
            String nodePropertyKey )
    {
        if ( relationshipPropertyKey == null ^ nodePropertyKey == null )
        {
            this.firstLabel = firstLabel;
            this.secondLabel = secondLabel;
            this.relationshipType = relationshipType;
            this.direction = direction;
            this.relationshipPropertyKey =
                    relationshipPropertyKey == null ? null : relationshipPropertyKey;
            this.nodePropertyKey = nodePropertyKey == null ? null : nodePropertyKey;
        }
        else
        {
            throw new IllegalArgumentException(
                    "Can not create description with property on both relationship and node" );
        }
    }

    public String getDescription()
    {
        if ( description == null )
        {
            StringBuilder builder = new StringBuilder();
            builder.append( "(:" ).append( firstLabel ).append( ")" );
            String prefix;
            String suffix;

            if ( direction.equals( Direction.OUTGOING ) )
            {
                prefix = "";
                suffix = ">";
            }
            else if ( direction.equals( Direction.INCOMING ) )
            {
                prefix = "<";
                suffix = "";
            }
            else
            {
                prefix = suffix = "";
            }
            builder.append( prefix ).append( "-[:" ).append( relationshipType );
            if ( relationshipPropertyKey != null )
            {
                appendProperty( builder, relationshipPropertyKey );
            }
            builder.append( "]-" ).append( suffix ).append( "(:" ).append( secondLabel );
            if ( nodePropertyKey != null )
            {
                appendProperty( builder, nodePropertyKey );
            }
            builder.append( ")" );
            description = builder.toString();
        }
        return description;
    }

    private void appendProperty( StringBuilder builder, String propertyKey )
    {
        builder.append( "{" ).append( propertyKey ).append( "}" );
    }

    @Override
    public int hashCode()
    {
        return getDescription().hashCode();
    }

    @Override
    public boolean equals( Object obj ) {
        if ( !( obj instanceof SCIndexDescription) )
            return false;
        if ( obj == this )
            return true;

        SCIndexDescription rhs = (SCIndexDescription) obj;
        return getDescription().equals( rhs.getDescription() );
    }

    @Override
    public String toString()
    {
        return getDescription();
    }
}
