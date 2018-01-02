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

import org.neo4j.graphmatching.PatternNode;

/**
 * An implementation which can compare commons expressions, for example
 * less than (&lt;), greater than or equal to (&gt;=) a.s.o.
 */
@Deprecated
public class CompareExpression extends AbstractFilterExpression
{
    private final String operator;
    private final Object compareValue;
    
    /**
     * Constructs a new comparison expression.
     * @param label the {@link PatternNode} label.
     * @param property property key.
     * @param operator operator, f.ex. &gt;= or &lt; or =
     * @param value value to compare against.
     */
    public CompareExpression( String label, String property, String operator,
        Object value )
    {
        super( label, property );
        this.operator = operator;
        this.compareValue = value;
    }
    
    public boolean matches( FilterValueGetter valueGetter )
    {
        for ( Object value : valueGetter.getValues( getLabel() ) )
        {
            int comparison = 0;
            try
            {
                comparison = ( ( Comparable<Object> ) value ).compareTo(
                    ( ( Comparable<Object> ) this.compareValue ) );
            }
            catch ( Exception e )
            {
                comparison = value.toString().compareTo(
                    this.compareValue.toString() );
            }
            boolean match = false;
            if ( operator.equals( "<" ) )
            {
                match = comparison < 0;
            }
            else if ( operator.equals( "<=" ) )
            {
                match = comparison <= 0;
            }
            else if ( operator.equals( "=" ) )
            {
                match = comparison == 0;
            }
            else if ( operator.equals( ">=" ) )
            {
                match = comparison >= 0;
            }
            else if ( operator.equals( ">" ) )
            {
                match = comparison > 0;
            }
            if ( match )
            {
                return true;
            }
        }
        return false;
    }
}
