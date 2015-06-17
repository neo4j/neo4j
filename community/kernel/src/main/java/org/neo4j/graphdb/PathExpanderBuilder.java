/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.graphdb;

import org.neo4j.kernel.StandardExpander;

import static org.neo4j.graphdb.Direction.BOTH;

/**
 * A fluent builder for creating specialized {@link PathExpander path expanders}.
 * <p>
 * See {@link PathExpanders} for a catalog of common expanders.
 */
public class PathExpanderBuilder
{
    /**
     * A {@link PathExpanderBuilder} that follows no relationships. You start with this and use
     * {@link #add(RelationshipType, Direction)} to form a restrictive PathExpander with just a few expansion rules
     * in it.
     */
    public static PathExpanderBuilder empty()
    {
        return new PathExpanderBuilder( StandardExpander.EMPTY );
    }

    /**
     * A {@link PathExpanderBuilder} that is seeded with all possible relationship types in {@link Direction#BOTH both
     * directions}. You start with this and {@link #remove(RelationshipType) remove types} to form a permissive
     * {@link PathExpander} with just a few exceptions in it.
     */
    public static <STATE> PathExpanderBuilder allTypesAndDirections()
    {
        return new PathExpanderBuilder( StandardExpander.DEFAULT );
    }

    /**
     * A {@link PathExpanderBuilder} seeded with all possible types but restricted to {@code direction}. You start
     * with this and {@link #remove(RelationshipType) remove types} to form a permissive {@link PathExpander} with
     * just a few exceptions in it.
     *
     * @param direction The direction you want to restrict expansions to
     */
    public static PathExpanderBuilder allTypes( Direction direction )
    {
        return new PathExpanderBuilder( StandardExpander.create( direction ) );
    }

    /**
     * Add a pair of {@code type} and {@link Direction#BOTH} to the PathExpander configuration.
     */
    public PathExpanderBuilder add( RelationshipType type )
    {
        return add( type, BOTH );
    }

    /**
     * Add a pair of {@code type} and {@code direction} to the PathExpander configuration.
     */
    public PathExpanderBuilder add( RelationshipType type, Direction direction )
    {
        return new PathExpanderBuilder( expander.add( type, direction ) );
    }

    /**
     * Remove expansion of {@code type} in any direction from the PathExpander configuration.
     * <p>
     * Example: {@code PathExpanderBuilder.allTypesAndDirections().remove(type).add(type, Direction.INCOMING)}
     * would restrict the {@link PathExpander} to only follow {@code Direction.INCOMING} relationships for {@code
     * type} while following any other relationship type in either direction.
     */
    public PathExpanderBuilder remove( RelationshipType type )
    {
        return new PathExpanderBuilder( expander.remove( type ) );
    }

    /**
     * Produce a PathExpander from the configuration you have built up.
     */
    @SuppressWarnings("unchecked")
    public <STATE> PathExpander<STATE> build()
    {
        return expander;
    }

    private final StandardExpander expander;

    private PathExpanderBuilder( StandardExpander expander )
    {
        this.expander = expander;
    }
}
