/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.visualization.graphviz.color;

/**
 * Color palette from: http://tango.freedesktop.org/Tango_Icon_Theme_Guidelines
 * The palette is in public domain.
 */
public enum Color
{
    GREY( "#2e3436", "#888a85" ),
    GREEN( "#4e9a06", "#73d216" ),
    RED( "#a40000", "#cc0000" ),
    BLUE( "#204a87", "#3465a4" ),
    BROWN( "#8f5902", "#c17d11" ),
    PURPLE( "#5c3566", "#75507b" ),
    YELLOW( "#c4a000", "#edd400" ),
    ORANGE( "#ce5c00", "#f57900" );

    String dark;
    String light;

    Color( String dark, String light )
    {
        this.dark = dark;
        this.light = light;
    }
}