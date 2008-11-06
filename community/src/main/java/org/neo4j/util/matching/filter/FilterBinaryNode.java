package org.neo4j.util.matching.filter;

/**
 * Matches two {@link FilterExpression}s with AND or OR.
 */
public class FilterBinaryNode implements FilterExpression
{
    private FilterExpression e1;
    private FilterExpression e2;
    private boolean trueForAnd;
    
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
    
    public FilterExpression getLeftExpression()
    {
        return this.e1;
    }
    
    public FilterExpression getRightExpression()
    {
        return this.e2;
    }
}
