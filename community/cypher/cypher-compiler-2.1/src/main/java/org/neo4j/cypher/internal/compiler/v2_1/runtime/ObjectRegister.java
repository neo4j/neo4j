package org.neo4j.cypher.internal.compiler.v2_1.runtime;

public final class ObjectRegister implements Register
{
    private final Registers registers;
    private final int idx;

    public ObjectRegister( Registers registers, int idx )
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
        return registers.getObjectRegister( idx );
    }

    @Override
    public void setObject( Object value )
    {
        registers.setObjectRegister( idx, value );
    }

    @Override
    public void copyFrom( Registers registers )
    {
        setObject( registers.getObjectRegister( idx ) );
    }

    @Override
    public void copyTo( Registers registers )
    {
        registers.setObjectRegister( idx, getObject() );
    }
}
