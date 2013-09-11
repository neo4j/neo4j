/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

/**
 * A catalogue of convenient {@link PathExpander} factory methods. Copied from kernel package so that we can hide
 * kernel from the public API.
 * <p/>
 * Use {@link PathExpanderBuilder} to build specialised {@link PathExpander}s.
 */
public abstract class PathExpanders
{
    /**
     * A very permissive {@link PathExpander} that follows any type in any direction.
     */
    @SuppressWarnings("unchecked")
    public static <STATE> PathExpander<STATE> allTypesAndDirections()
    {
        return StandardExpander.DEFAULT;
    }

    /**
     * A very permissive {@link PathExpander} that follows {@code type} in any direction.
     */
    @SuppressWarnings("unchecked")
    public static <STATE> PathExpander<STATE> forType( RelationshipType type )
    {
        return StandardExpander.create( type, Direction.BOTH );
    }

    /**
     * A very permissive {@link PathExpander} that follows any type in {@code direction}.
     */
    @SuppressWarnings("unchecked")
    public static <STATE> PathExpander<STATE> forDirection( Direction direction )
    {
        return StandardExpander.create( direction );
    }

    /**
     * A very restricted {@link PathExpander} that follows {@code type} in {@code direction}.
     */
    @SuppressWarnings("unchecked")
    public static <STATE> PathExpander<STATE> forTypeAndDirection( RelationshipType type, Direction direction )
    {
        return StandardExpander.create( type, direction );
    }

    /**
     * A very restricted {@link PathExpander} that follows only the {@code type}/ {@code direction} pairs that you list.
     */
    @SuppressWarnings("unchecked")
    public static <STATE> PathExpander<STATE> forTypesAndDirections( RelationshipType type1, Direction direction1,
                                                                     RelationshipType type2, Direction direction2,
                                                                     Object... more )
    {
        return StandardExpander.create( type1, direction1, type2, direction2, more );
    }

    private PathExpanders()
    {
        // you should never instantiate this
    }
}
