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

/**
 * Matches two {@link FilterExpression}s with AND or OR.
 */
@Deprecated
public class FilterBinaryNode implements FilterExpression
{
    private FilterExpression e1;
    private FilterExpression e2;
    private boolean trueForAnd;
    
    /**
     * Constructs a new binary node which has two expressions, grouped together
     * as one.
     * @param expression1 the first expression.
     * @param trueForAnd {@code true} if AND, else OR.
     * @param expression2 the second expression.
     */
    public FilterBinaryNode( FilterExpression expression1,
        boolean trueForAnd, FilterExpression expression2 )
    {
        this.e1 = expression1;
        this.e2 = expression2;
        this.trueForAnd = trueForAnd;
    }
    
    public boolean matches( FilterValueGetter valueGetter )
    {
        return this.trueForAnd ?
            this.e1.matches( valueGetter ) && this.e2.matches( valueGetter ) :
            this.e1.matches( valueGetter ) || this.e2.matches( valueGetter );
    }
    
    /**
     * @return the first expression of the two.
     */
    public FilterExpression getLeftExpression()
    {
        return this.e1;
    }
    
    /**
     * @return the second expression of the two.
     */
    public FilterExpression getRightExpression()
    {
        return this.e2;
    }
}
