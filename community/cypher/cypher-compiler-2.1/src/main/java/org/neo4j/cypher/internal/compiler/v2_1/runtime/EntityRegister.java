package org.neo4j.cypher.internal.compiler.v2_1.runtime;

public final class EntityRegister implements Register
{
    private final Registers registers;
    private final int idx;

    public EntityRegister( Registers registers, int idx )
    {
        this.registers = registers;
        this.idx = idx;
    }

    @Override
    public Registers registers()
    {
        return registers;
    }

    @Override
    public Object getObject()
    {
        return (Long) getEntity();
    }

    @Override
    public void setObject( Object value )
    {
        setEntity( (Long) value );
    }

    public long getEntity()
    {
        return registers.getEntityRegister( idx );
    }

    public void setEntity( long value )
    {
        registers.setEntityRegister( idx, value );
    }

    @Override
    public void copyFrom( Registers registers )
    {
        setEntity( registers.getEntityRegister( idx ) );
    }

    @Override
    public void copyTo( Registers registers )
    {
        registers.setEntityRegister( idx, getEntity() );
    }
}
