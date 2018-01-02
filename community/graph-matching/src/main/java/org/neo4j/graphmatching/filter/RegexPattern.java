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

import java.util.regex.Pattern;

import org.neo4j.graphmatching.PatternNode;

/**
 * A regex pattern with or without options, f.ex. "i" means case-insensitive.
 */
@Deprecated
public class RegexPattern extends AbstractFilterExpression
{
    private final Pattern pattern;
    
    /**
     * Constructs a new regex pattern for filtering.
     * 
     * @param label the {@link PatternNode} label.
     * @param property the property key to filter in.
     * @param pattern the pattern which the value should match.
     * @param options options for regex matching.
     */
    public RegexPattern( String label, String property, String pattern,
        String options )
    {
        super( label, property );
        int op = 0;
        op |= hasOption( options, 'i' ) ? Pattern.CASE_INSENSITIVE : 0;
        this.pattern = Pattern.compile( pattern, op );
    }

    public boolean matches( FilterValueGetter valueGetter )
    {
        Object values[] = valueGetter.getValues( getLabel() );
        for ( Object value : values )
        {
            boolean matches = this.pattern.matcher( value.toString() ).find();
            if ( matches )
            {
                return true;
            }
        }
        return false;
    }
    
    private static boolean hasOption( String options, char option )
    {
        return options != null && options.indexOf( option ) > -1;
    }
}
