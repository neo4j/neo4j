package org.neo4j.codegen;

/**
 * Default implementation of {@link ExpressionVisitor}
 */
public abstract class BaseExpressionVisitor implements ExpressionVisitor
{
    @Override
    public void invoke( Expression target, MethodReference method, Expression[] arguments )
    {

    }

    @Override
    public void invoke( MethodReference method, Expression[] arguments )
    {

    }

    @Override
    public void load( LocalVariable variable )
    {

    }

    @Override
    public void getField( Expression target, FieldReference field )
    {

    }

    @Override
    public void constant( Object value )
    {

    }

    @Override
    public void getStatic( FieldReference field )
    {

    }

    @Override
    public void loadThis( String sourceName )
    {

    }

    @Override
    public void newInstance( TypeReference type )
    {

    }

    @Override
    public void not( Expression expression )
    {

    }

    @Override
    public void ternary( Expression test, Expression onTrue, Expression onFalse )
    {

    }

    @Override
    public void eq( Expression lhs, Expression rhs )
    {

    }

    @Override
    public void or( Expression lhs, Expression rhs )
    {

    }

    @Override
    public void add( Expression lhs, Expression rhs )
    {

    }

    @Override
    public void gt( Expression lhs, Expression rhs )
    {

    }

    @Override
    public void sub( Expression lhs, Expression rhs )
    {

    }

    @Override
    public void cast( TypeReference type, Expression expression )
    {

    }
}
