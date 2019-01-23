package org.neo4j.kernel.impl.proc;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Field;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * On calling apply, injects the `value` for the field `field` on the provided `object`.
 */
public class FieldSetter
{
    private final Field field;
    private final MethodHandle setter;
    private final ComponentRegistry.Provider<?> provider;

    FieldSetter( Field field, MethodHandle setter, ComponentRegistry.Provider<?> provider )
    {
        this.field = field;
        this.setter = setter;
        this.provider = provider;
    }

    public void apply( org.neo4j.kernel.api.proc.Context ctx, Object object ) throws ProcedureException
    {
        try
        {
            setter.invoke( object, provider.apply( ctx ) );
        }
        catch ( Throwable e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, e,
                    "Unable to inject component to field `%s`, please ensure it is public and non-final: %s",
                    field.getName(), e.getMessage() );
        }
    }

    public Object get( org.neo4j.kernel.api.proc.Context ctx ) throws ProcedureException
    {
        try
        {
            return provider.apply( ctx );
        }
        catch ( Throwable e )
        {
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed, e,
                    "Unable to inject component to field `%s`, please ensure it is public and non-final: %s",
                    field.getName(), e.getMessage() );
        }
    }

    Field field()
    {
        return field;
    }
}
