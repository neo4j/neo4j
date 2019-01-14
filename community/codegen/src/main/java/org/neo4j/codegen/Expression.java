/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.codegen;

import java.util.Arrays;

import org.neo4j.values.AnyValue;

import static org.neo4j.codegen.TypeReference.BOOLEAN;
import static org.neo4j.codegen.TypeReference.DOUBLE;
import static org.neo4j.codegen.TypeReference.INT;
import static org.neo4j.codegen.TypeReference.LONG;
import static org.neo4j.codegen.TypeReference.OBJECT;
import static org.neo4j.codegen.TypeReference.VALUE;
import static org.neo4j.codegen.TypeReference.arrayOf;
import static org.neo4j.codegen.TypeReference.typeReference;

public abstract class Expression extends ExpressionTemplate
{
    public static final Expression TRUE = new Constant( BOOLEAN, Boolean.TRUE )
    {
        @Override
        Expression not()
        {
            return FALSE;
        }
    };
    public static final Expression FALSE = new Constant( BOOLEAN, Boolean.FALSE )
    {
        @Override
        Expression not()
        {
            return TRUE;
        }
    };
    public static final Expression NULL = new Constant( OBJECT, null );

    protected Expression( TypeReference type )
    {
        super( type );
    }

    public abstract void accept( ExpressionVisitor visitor );

    static final Expression SUPER = new Expression( OBJECT )
    {
        @Override
        public void accept( ExpressionVisitor visitor )
        {
            visitor.loadThis( "super" );
        }
    };

    public static Expression gt( final Expression lhs, final Expression rhs )
    {
        return new Expression( BOOLEAN )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.gt( lhs, rhs );
            }

            @Override
            Expression not()
            {
                return lte( lhs, rhs );
            }
        };
    }

    public static Expression gte( final Expression lhs, final Expression rhs )
    {
        return new Expression( BOOLEAN )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.gte( lhs, rhs );
            }

            @Override
            Expression not()
            {
                return lt( lhs, rhs );
            }
        };
    }

    public static Expression lt( final Expression lhs, final Expression rhs )
    {
        return new Expression( BOOLEAN )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.lt( lhs, rhs );
            }

            @Override
            Expression not()
            {
                return gte( lhs, rhs );
            }
        };
    }

    public static Expression lte( final Expression lhs, final Expression rhs )
    {
        return new Expression( BOOLEAN )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.lte( lhs, rhs );
            }

            @Override
            Expression not()
            {
                return gt( lhs, rhs );
            }
        };
    }

    public static Expression and( final Expression lhs, final Expression rhs )
    {
        if ( lhs == FALSE || rhs == FALSE )
        {
            return FALSE;
        }
        if ( lhs == TRUE )
        {
            return rhs;
        }
        if ( rhs == TRUE )
        {
            return lhs;
        }
        Expression[] expressions;
        if ( lhs instanceof And )
        {
            if ( rhs instanceof And )
            {
                expressions = expressions( ((And) lhs).expressions, ((And) rhs).expressions );
            }
            else
            {
                expressions = expressions( ((And) lhs).expressions, rhs );
            }
        }
        else if ( rhs instanceof And )
        {
            expressions = expressions( lhs, ((And) rhs).expressions );
        }
        else
        {
            expressions = new Expression[] {lhs, rhs};
        }
        return new And( expressions );
    }

    public static Expression or( final Expression lhs, final Expression rhs )
    {
        if ( lhs == TRUE || rhs == TRUE )
        {
            return TRUE;
        }
        if ( lhs == FALSE )
        {
            return rhs;
        }
        if ( rhs == FALSE )
        {
            return lhs;
        }
        Expression[] expressions;
        if ( lhs instanceof Or )
        {
            if ( rhs instanceof Or )
            {
                expressions = expressions( ((Or) lhs).expressions, ((Or) rhs).expressions );
            }
            else
            {
                expressions = expressions( ((Or) lhs).expressions, rhs );
            }
        }
        else if ( rhs instanceof Or )
        {
            expressions = expressions( lhs, ((Or) rhs).expressions );
        }
        else
        {
            expressions = new Expression[] {lhs, rhs};
        }
        return new Or( expressions );
    }

    private static class And extends Expression
    {
        private final Expression[] expressions;

        And( Expression[] expressions )
        {
            super( BOOLEAN );
            this.expressions = expressions;
        }

        @Override
        public void accept( ExpressionVisitor visitor )
        {
            visitor.and( expressions );
        }
    }

    private static class Or extends Expression
    {
        private final Expression[] expressions;

        Or( Expression[] expressions )
        {
            super( BOOLEAN );
            this.expressions = expressions;
        }

        @Override
        public void accept( ExpressionVisitor visitor )
        {
            visitor.or( expressions );
        }
    }

    private static Expression[] expressions( Expression[] some, Expression[] more )
    {
        Expression[] result = Arrays.copyOf( some, some.length + more.length );
        System.arraycopy( more, 0, result, some.length, more.length );
        return result;
    }

    private static Expression[] expressions( Expression[] some, Expression last )
    {
        Expression[] result = Arrays.copyOf( some, some.length + 1 );
        result[some.length] = last;
        return result;
    }

    private static Expression[] expressions( Expression first, Expression[] more )
    {
        Expression[] result = new Expression[more.length + 1];
        result[0] = first;
        System.arraycopy( more, 0, result, 1, more.length );
        return result;
    }

    public static Expression equal( final Expression lhs, final Expression rhs )
    {
        if ( lhs == NULL )
        {
            if ( rhs == NULL )
            {
                return constant( Boolean.TRUE );
            }
            else
            {
                return isNull( rhs );
            }
        }
        else if ( rhs == NULL )
        {
            return isNull( lhs );
        }
        return new Expression( BOOLEAN )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.equal( lhs, rhs );
            }

            @Override
            Expression not()
            {
                return notEqual( lhs, rhs );
            }
        };
    }

    public static Expression isNull( final Expression expression )
    {
        return new Expression( BOOLEAN )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.isNull( expression );
            }

            @Override
            Expression not()
            {
                return notNull( expression );
            }
        };
    }

    public static Expression notNull( final Expression expression )
    {
        return new Expression( BOOLEAN )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.notNull( expression );
            }

            @Override
            Expression not()
            {
                return isNull( expression );
            }
        };
    }

    public static Expression notEqual( final Expression lhs, final Expression rhs )
    {
        return new Expression( BOOLEAN )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.notEqual( lhs, rhs );
            }

            @Override
            Expression not()
            {
                return equal( lhs, rhs );
            }
        };
    }

    public static Expression load( final LocalVariable variable )
    {
        return new Expression( variable.type() )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.load( variable );
            }
        };
    }

    public static Expression add( final Expression lhs, final Expression rhs )
    {
        if ( !lhs.type.equals( rhs.type ) )
        {
            throw new IllegalArgumentException(
                    String.format( "Cannot add variables with different types. LHS %s, RHS %s", lhs.type.simpleName(),
                            rhs.type.simpleName() ) );
        }

        return new Expression( lhs.type )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.add( lhs, rhs );
            }
        };
    }

    public static Expression subtract( final Expression lhs, final Expression rhs )
    {
        if ( !lhs.type.equals( rhs.type ) )
        {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot subtract variables with different types. LHS %s, RHS %s",
                            lhs.type.simpleName(),
                            rhs.type.simpleName() ) );
        }
        return new Expression( lhs.type )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.subtract( lhs, rhs );
            }
        };
    }

    public static Expression multiply( final Expression lhs, final Expression rhs )
    {
        if ( !lhs.type.equals( rhs.type ) )
        {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot multiply variables with different types. LHS %s, RHS %s",
                            lhs.type.simpleName(),
                            rhs.type.simpleName() ) );
        }
        return new Expression( lhs.type )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.multiply( lhs, rhs );
            }
        };
    }

    public static Expression constantInt( int value )
    {
        return constant(value);
    }

    public static Expression constantLong( long value )
    {
        return constant(value);
    }

    public static Expression constant( final Object value )
    {
        TypeReference reference;
        if ( value == null )
        {
            return NULL;
        }
        else if ( value instanceof String )
        {
            reference = TypeReference.typeReference( String.class );
        }
        else if ( value instanceof Long )
        {
            reference = LONG;
        }
        else if ( value instanceof Integer )
        {
            reference = INT;
        }
        else if ( value instanceof Double )
        {
            reference = DOUBLE;
        }
        else if ( value instanceof Boolean )
        {
            return (Boolean) value ? TRUE : FALSE;
        }
        else if ( value instanceof AnyValue )
        {
            reference = VALUE;
        }
        else
        {
            throw new IllegalArgumentException( "Not a valid constant!" );
        }

        return new Expression( reference )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.constant( value );
            }
        };
    }

    private static class Constant extends Expression
    {
        private final Object value;

        Constant( TypeReference type, Object value )
        {
            super( type );
            this.value = value;
        }

        @Override
        public void accept( ExpressionVisitor visitor )
        {
            visitor.constant( value );
        }
    }

    //TODO deduce type from constants
    public static Expression newArray( TypeReference baseType, Expression... constants )
    {
        return new Expression( arrayOf( baseType ) )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.newArray( baseType, constants );
            }
        };
    }

    /** get instance field */
    public static Expression get( final Expression target, final FieldReference field )
    {
        return new Expression( field.type() )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.getField( target, field );
            }
        };
    }

    /** box expression */
    public static Expression box( final Expression expression )
    {
        TypeReference type = expression.type;
        if ( type.isPrimitive() )
        {
            switch ( type.name() )
            {
            case "byte":
                type = TypeReference.typeReference( Byte.class );
                break;
            case "short":
                type = TypeReference.typeReference( Short.class );
                break;
            case "int":
                type = TypeReference.typeReference( Integer.class );
                break;
            case "long":
                type = TypeReference.typeReference( Long.class );
                break;
            case "char":
                type = TypeReference.typeReference( Character.class );
                break;
            case "boolean":
                type = TypeReference.typeReference( Boolean.class );
                break;
            case "float":
                type = TypeReference.typeReference( Float.class );
                break;
            case "double":
                type = TypeReference.typeReference( Double.class );
                break;
            default:
                break;
            }
        }
        return new Expression( type )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.box( expression );
            }
        };
    }

    /** unbox expression */
    public static Expression unbox( final Expression expression )
    {
        TypeReference type;
        switch ( expression.type.fullName() )
        {
        case "java.lang.Byte":
            type = TypeReference.typeReference( byte.class );
            break;
        case "java.lang.Short":
            type = TypeReference.typeReference( short.class );
            break;
        case "java.lang.Integer":
            type = TypeReference.typeReference( int.class );
            break;
        case "java.lang.Long":
            type = TypeReference.typeReference( long.class );
            break;
        case "java.lang.Character":
            type = TypeReference.typeReference( char.class );
            break;
        case "java.lang.Boolean":
            type = TypeReference.typeReference( boolean.class );
            break;
        case "java.lang.Float":
            type = TypeReference.typeReference( float.class );
            break;
        case "java.lang.Double":
            type = TypeReference.typeReference( double.class );
            break;
        default:
            throw new IllegalStateException( "Cannot unbox " + expression.type.fullName() );
        }
        return new Expression( type )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.unbox( expression );
            }
        };
    }

    /** get static field */
    public static Expression getStatic( final FieldReference field )
    {
        return new Expression( field.type() )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.getStatic( field );
            }
        };
    }

    public static Expression ternary( final Expression test, final Expression onTrue, final Expression onFalse )
    {
        TypeReference reference = onTrue.type.equals( onFalse.type ) ? onTrue.type : OBJECT;
        return new Expression( reference )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.ternary( test, onTrue, onFalse );
            }
        };
    }

    public static Expression invoke(
            final Expression target, final MethodReference method,
            final Expression... arguments )
    {
        return new Expression( method.returns() )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.invoke( target, method, arguments );
            }
        };
    }

    public static Expression invoke( final MethodReference method, final Expression... parameters )
    {
        return new Expression( method.returns() )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.invoke( method, parameters );
            }
        };
    }

    public static Expression cast( Class<?> type, Expression expression )
    {
        return cast( typeReference( type ), expression );
    }

    public static Expression cast( final TypeReference type, Expression expression )
    {
        return new Expression( type )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.cast( type, expression );
            }
        };
    }

    public static Expression newInstance( Class<?> type )
    {
        return newInstance( typeReference( type ) );
    }

    public static Expression newInstance( final TypeReference type )
    {
        return new Expression( type )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.newInstance( type );
            }
        };
    }

    public static Expression not( final Expression expression )
    {
        return expression.not();
    }

    Expression not()
    {
        return notExpr( this );
    }

    private static Expression notExpr( final Expression expression )
    {
        assert expression.type == BOOLEAN : "Can only apply not() to boolean expressions";
        return new Expression( BOOLEAN )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.not( expression );
            }

            @Override
            Expression not()
            {
                return expression;
            }
        };
    }

    public static Expression toDouble( final Expression expression )
    {
        return new Expression( DOUBLE )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.longToDouble( expression );
            }
        };
    }

    public static Expression pop( Expression expression )
    {
        return new Expression( expression.type )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.pop( expression );
            }
        };
    }

    @Override
    Expression materialize( CodeBlock method )
    {
        return this;
    }

    @Override
    void templateAccept( CodeBlock method, ExpressionVisitor visitor )
    {
        throw new UnsupportedOperationException( "simple expressions should not be invoked as templates" );
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder().append( "Expression[" );
        accept( new ExpressionToString( result ) );
        return result.append( ']' ).toString();
    }
}
