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
package org.neo4j.visualization.graphviz.color;

import java.util.Collection;

/**
 * Map from entities to colors, and list what colors are used/reserved.
 * 
 * @author anders
 * 
 * @param <E> the type to map to colors
 */
public interface ColorMapper<E>
{
    /**
     * Get color for an entity.
     * 
     * @param entity entity to get color for
     * @return color for entity
     */
    Color getColor( E entity );

    /**
     * Colors to reserve - note that it will only be called once, any changes to
     * the collection after that will have no effect.
     * 
     * @return reserved colors
     */
    Collection<Color> getColors();
}
