package org.neo4j.util.matching.filter;

public class CompareExpression extends AbstractFilterExpression
{
    private final String operator;
    private final Object compareValue;
    
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
