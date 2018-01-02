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
package org.neo4j.graphmatching;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.graphmatching.filter.FilterExpression;

/**
 * Represents a group in SPARQL. A group can carry filters, which can restrict
 * the returned result set.
 */
@Deprecated
public class PatternGroup
{
    private Collection<FilterExpression> regexExpression =
        new ArrayList<FilterExpression>();
    
    /**
     * Adds a filter expression to the list of filters for this group.
     * @param regexRepression the {@link FilterExpression} to add to this
     * group.
     */
    public void addFilter( FilterExpression regexRepression )
    {
        this.regexExpression.add( regexRepression );
    }
    
    /**
     * Returns the filter expressions which has been added for this group with
     * {@link #addFilter(FilterExpression)}.
     * @return the filters for this group.
     */
    public FilterExpression[] getFilters()
    {
        return this.regexExpression.toArray(
            new FilterExpression[ this.regexExpression.size() ] );
    }
}
