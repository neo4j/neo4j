package org.neo4j.graphalgo;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public final class Path
{
    public static final class Builder
    {
        private final Builder previous;
        private final Node start;
        private final Relationship relationship;
        private final int size;

        public Builder( Node start )
        {
            if ( start == null )
            {
                throw new NullPointerException();
            }
            this.start = start;
            this.previous = null;
            this.relationship = null;
            this.size = 0;
        }

        private Builder( Builder prev, Relationship rel )
        {
            this.start = prev.start;
            this.previous = prev;
            this.relationship = rel;
            this.size = prev.size + 1;
        }

        public Node getStartNode()
        {
            return start;
        }

        public Path build()
        {
            return new Path( this, null );
        }

        public Builder push( Relationship relationship )
        {
            if ( relationship == null )
            {
                throw new NullPointerException();
            }
            return new Builder( this, relationship );
        }

        public Path build( Builder other )
        {
            return new Path( this, other );
        }

        @Override
        public String toString()
        {
            if ( previous == null )
            {
                return start.toString();
            }
            else
            {
                return relToString( relationship ) + ":" + previous.toString();
            }
        }
    }

    private static String relToString( Relationship rel )
    {
        return rel.getStartNode() + "--" + rel.getType() + "-->"
               + rel.getEndNode();
    }

    private final Node start;
    private final Relationship[] path;

    private Path( Builder left, Builder right )
    {
        // System.out.println( left );
        // System.out.println( right );
        path = new Relationship[left.size + ( right == null ? 0 : right.size )];
        if ( right != null )
        {
            for ( int i = left.size, total = i + right.size; i < total; i++ )
            {
                path[i] = right.relationship;
                right = right.previous;
            }
            assert right.relationship == null : "right Path.Builder size error";
        }
        for ( int i = left.size - 1; i >= 0; i-- )
        {
            path[i] = left.relationship;
            left = left.previous;
        }
        assert left.relationship == null : "left Path.Builder size error";
        start = left.start;
    }

    public static Path singular( Node start )
    {
        return new Builder( start ).build();
    }

    public Node getStartNode()
    {
        return start;
    }

    public Iterable<Node> getNodes()
    {
        return new Iterable<Node>()
        {
            public Iterator<Node> iterator()
            {
                return new Iterator<Node>()
                {
                    Node current = start;
                    int index = 0;

                    public boolean hasNext()
                    {
                        return index <= path.length;
                    }

                    public Node next()
                    {
                        if ( current == null )
                        {
                            throw new NoSuchElementException();
                        }
                        Node next = null;
                        if ( index < path.length )
                        {
                            next = path[index].getOtherNode( current );
                        }
                        index += 1;
                        try
                        {
                            return current;
                        }
                        finally
                        {
                            current = next;
                        }
                    }

                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public Iterable<Relationship> getRelationships()
    {
        return Collections.unmodifiableCollection( Arrays.asList( path ) );
    }

    public int length()
    {
        return path.length;
    }

    @Override
    public int hashCode()
    {
        if ( path.length == 0 )
        {
            return start.hashCode();
        }
        else
        {
            return Arrays.hashCode( path );
        }
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        else if ( obj instanceof Path )
        {
            Path other = (Path) obj;
            return start.equals( other.start )
                   && Arrays.equals( path, other.path );
        }
        else
        {
            return false;
        }
    }

    @Override
    public String toString()
    {
        Node current = start;
        StringBuilder result = new StringBuilder();
        for ( Relationship rel : path )
        {
            result.append( current );
            String prefix = "--", suffix = "--";
            if ( current.equals( rel.getEndNode() ) )
                prefix = "<--";
            else
                suffix = "-->";
            result.append( prefix );
            result.append( rel.getType() );
            result.append( ".[" );
            result.append( rel.getId() );
            result.append( "]" );
            result.append( suffix );
            current = rel.getOtherNode( current );
        }
        result.append( current );
        return result.toString();
    }
}
