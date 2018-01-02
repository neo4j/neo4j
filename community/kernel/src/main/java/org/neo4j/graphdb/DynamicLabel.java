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
package org.neo4j.graphdb;

/**
 * A dynamically instantiated and named {@link Label}. This class is
 * a convenience implementation of <code>Label</code> that is
 * typically used when labels are created and named after a
 * condition that can only be detected at runtime.
 * 
 * For statically known labels please consider the enum approach as described
 * in {@link Label} documentation.
 *
 * @see Label
 */
public class DynamicLabel implements Label
{
    /**
     * @param labelName the name of the label.
     * @return a {@link Label} instance for the given {@code labelName}.
     */
    public static Label label( String labelName )
    {
        return new DynamicLabel( labelName );
    }

    private final String name;

    private DynamicLabel( String labelName )
    {
        this.name = labelName;
    }

    @Override
    public String name()
    {
        return this.name;
    }

    @Override
    public String toString()
    {
        return this.name;
    }

    @Override
    public boolean equals(Object other)
    {
        return other instanceof Label && ((Label) other).name().equals( name );
    }

    @Override
    public int hashCode()
    {
        return 26578 ^ name.hashCode();
    }
}
