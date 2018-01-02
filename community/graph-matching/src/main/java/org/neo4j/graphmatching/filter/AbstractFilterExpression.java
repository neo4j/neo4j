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
package org.neo4j.graphmatching.filter;

import org.neo4j.graphmatching.PatternElement;
import org.neo4j.graphmatching.PatternNode;

/**
 * Abstract class which contains {@link PatternElement} label and property key.
 */
@Deprecated
public abstract class AbstractFilterExpression implements FilterExpression
{
    private final String label;
    private final String property;
    
    /**
     * Constructs a new filter expression.
     * @param label the {@link PatternNode} label.
     * @param property the property key.
     */
    public AbstractFilterExpression( String label, String property )
    {
        this.label = label;
        this.property = property;
    }
    
    /**
     * @return The {@link PatternNode} label.
     */
    public String getLabel()
    {
        return this.label;
    }
    
    /**
     * @return the property key.
     */
    public String getProperty()
    {
        return this.property;
    }
}
