package org.neo4j.util.matching.regex;

/**
 * Matches two {@link RegexExpression}s with AND or OR.
 */
public class RegexBinaryNode implements RegexExpression
{
    private RegexExpression e1;
    private RegexExpression e2;
    private boolean trueForAnd;
    
    public RegexBinaryNode( RegexExpression expression1,
        boolean trueForAnd, RegexExpression expression2 )
    {
        this.e1 = expression1;
        this.e2 = expression2;
        this.trueForAnd = trueForAnd;
    }
    
    public boolean matches( RegexValueGetter valueGetter )
    {
        return this.trueForAnd ?
            this.e1.matches( valueGetter ) && this.e2.matches( valueGetter ) :
            this.e1.matches( valueGetter ) || this.e2.matches( valueGetter );
    }
    
    public RegexExpression getLeftExpression()
    {
        return this.e1;
    }
    
    public RegexExpression getRightExpression()
    {
        return this.e2;
    }
}
