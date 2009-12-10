package org.neo4j.shell.neo;

public class TypedId
{
    private final String type;
    private final long id;
    private final boolean isNode;
    
    public TypedId( String typedId )
    {
        this( typedId.substring( 0, 1 ),
            Long.parseLong( typedId.substring( 1 ) ) );
    }
    
    public TypedId( String type, long id )
    {
        this.type = type;
        this.id = id;
        this.isNode = type.equals( NodeOrRelationship.TYPE_NODE );
    }
    
    public String getType()
    {
        return this.type;
    }
    
    public long getId()
    {
        return this.id;
    }
    
    public boolean isNode()
    {
        return this.isNode;
    }
    
    public boolean isRelationship()
    {
        return !this.isNode;
    }
    
    @Override
    public boolean equals( Object o )
    {
        if ( !( o instanceof TypedId ) )
        {
            return false;
        }
        TypedId other = ( TypedId ) o;
        return this.type.equals( other.type ) &&
            this.id == other.id;
    }
    
    @Override
    public int hashCode()
    {
        int code = 7;
        code = 31 * code + new Long( this.id ).hashCode();
        code = 31 * code + this.type.hashCode();
        return code;
    }
    
    @Override
    public String toString()
    {
        return this.type + this.id;
    }
}
