/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import static org.neo4j.codegen.TypeReference.BOOLEAN;
import static org.neo4j.codegen.TypeReference.DOUBLE;
import static org.neo4j.codegen.TypeReference.INT;
import static org.neo4j.codegen.TypeReference.LONG;
import static org.neo4j.codegen.TypeReference.OBJECT;
import static org.neo4j.codegen.TypeReference.arrayOf;
import static org.neo4j.codegen.TypeReference.typeReference;

public abstract class Expression extends ExpressionTemplate
{
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
        };
    }

    public static Expression and( final Expression lhs, final Expression rhs )
    {
        return new Expression( BOOLEAN )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.and( lhs, rhs );
            }
        };
    }

    public static Expression or( final Expression lhs, final Expression rhs )
    {
        return new Expression( BOOLEAN )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.or( lhs, rhs );
            }
        };
    }

    public static Expression equal( final Expression lhs, final Expression rhs )
    {
        return new Expression( BOOLEAN )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.equal( lhs, rhs );
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
                            rhs.type.simpleName() ));
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
                    String.format( "Cannot subtract variables with different types. LHS %s, RHS %s", lhs.type.simpleName(),
                            rhs.type.simpleName() ));
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
                    String.format( "Cannot multiply variables with different types. LHS %s, RHS %s", lhs.type.simpleName(),
                            rhs.type.simpleName() ));
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

    public static Expression constant( final Object value )
    {

        TypeReference reference;
        if ( value == null )
        {
            reference = OBJECT;
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
            reference = BOOLEAN;
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
    public static Expression box( final Expression expression)
    {
        TypeReference type;
        switch ( expression.type.simpleName() )
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
            type = expression.type();
        }
        return new Expression( type )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.box(expression);
            }
        };
    }

    /** unbox expression */
    public static Expression unbox( final Expression expression)
    {
        TypeReference type;
        switch ( expression.type.name() )
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
            throw new IllegalStateException( "Cannot unbox " + expression.type.name() );
        }
        return new Expression( type )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.unbox(expression);
            }
        };
    }

    /** get static field */
    public static Expression getStatic(final FieldReference field )
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

    public static Expression ternaryOnNull( final Expression test, final Expression onTrue, final Expression onFalse )
    {
        TypeReference reference = onTrue.type.equals( onFalse.type ) ? onTrue.type : OBJECT;
        return new Expression( reference )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.ternaryOnNull( test, onTrue, onFalse );
            }
        };
    }

    public static Expression ternaryOnNonNull( final Expression test, final Expression onTrue,
            final Expression onFalse )
    {
        TypeReference reference = onTrue.type.equals( onFalse.type ) ? onTrue.type : OBJECT;
        return new Expression( reference )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.ternaryOnNonNull( test, onTrue, onFalse );
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

    public static Expression invoke( final Expression target, final MethodReference method,
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
        return new Expression( BOOLEAN )
        {
            @Override
            public void accept( ExpressionVisitor visitor )
            {
                visitor.not( expression );
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
