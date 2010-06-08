package org.neo4j.graphmatching.filter;

/**
 * Matches two {@link FilterExpression}s with AND or OR.
 */
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
