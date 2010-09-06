package org.neo4j.helpers;

public class Triplet<T1, T2, T3> extends Pair<T1, T2>
{
    private final T3 third;

    public Triplet( T1 first, T2 other, T3 third )
    {
        super( first, other );
        this.third = third;
    }

    public static <T1, T2, T3> Triplet<T1, T2, T3> of( T1 first, T2 other, T3 third )
    {
        return new Triplet<T1, T2, T3>( first, other, third );
    }

    public T3 third()
    {
        return this.third;
    }

    @Override
    public String toString()
    {
        return "(" + first() + ", " + other() + ", " + third + ")";
    }

    @Override
    public int hashCode()
    {
        return ( 31 * super.hashCode() ) | hashCode( third );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( obj instanceof Triplet )
        {
            if ( obj.getClass() != this.getClass() ) return false;
            Triplet that = (Triplet) obj;
            return this.pairEquals( that ) && equals( this.third, that.third );
        }
        return false;
    }
}
