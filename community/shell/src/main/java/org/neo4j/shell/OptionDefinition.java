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
package org.neo4j.shell;

/**
 * Groups an {@link OptionValueType} and a description.
 */
public class OptionDefinition
{
    private OptionValueType type;
    private String description;
    
    /**
     * @param type the type for the option.
     * @param description the description of the option.
     */
    public OptionDefinition( OptionValueType type, String description )
    {
        this.type = type;
        this.description = description;
    }
    
    /**
     * @return the option value type.
     */
    public OptionValueType getType()
    {
        return this.type;
    }
    
    /**
     * @return the description.
     */
    public String getDescription()
    {
        return this.description;
    }
}
