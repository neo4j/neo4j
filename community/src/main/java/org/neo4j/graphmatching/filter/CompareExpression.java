package org.neo4j.graphmatching.filter;

import org.neo4j.graphmatching.PatternNode;

/**
 * An implementation which can compare commons expressions, f.ex:
 * less than (<), greater than or equal to (>=) a.s.o.
 */
public class CompareExpression extends AbstractFilterExpression
{
    private final String operator;
    private final Object compareValue;
    
    /**
     * Constructs a new comparison expression.
     * @param label the {@link PatternNode} label.
     * @param property property key.
     * @param operator operator, f.ex. >= or < or =
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
