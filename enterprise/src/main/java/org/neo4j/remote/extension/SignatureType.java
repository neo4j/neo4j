package org.neo4j.remote.extension;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;

public abstract class SignatureType
{
    public static final SignatureType VOID = new SimpleType( TypeBase.VOID );
    public static final SignatureType BOOLEAN = new SimpleType(
        TypeBase.BOOLEAN );
    public static final SignatureType INTEGER = new SimpleType(
        TypeBase.INTEGER );
    public static final SignatureType STRING = new SimpleType( TypeBase.STRING );
    public static final SignatureType NODE = new SimpleType( TypeBase.NODE );
    public static final SignatureType RELATIONSHIP = new SimpleType(
        TypeBase.RELATIONSHIP );
    public static final SignatureType RELATIONSHIP_TYPE = new SimpleType(
        TypeBase.RELATIONSHIP_TYPE );
    public static final SignatureType PROPERTY_VALUE = new SimpleType(
        TypeBase.PROPERTY_VALUE );
    public static final SignatureType TRAVERSAL_POSITION = new SimpleType(
        TypeBase.TRAVERSAL_POSITION );

    public abstract <F, T> T dispatch( TypeProcessor<F, T> processor, F arg );

    public static interface TypeProcessor<F, T>
    {
        T processVoid( F arg );

        T processBoolean( F arg );

        T processInteger( F arg );

        T processFloat( F arg );

        T processString( F arg );

        T processNode( F arg );

        T processRelationship( F arg );

        T processRelationshipType( F arg );

        T processPropertyValue( F arg );

        T processArray( SignatureType contentType, F arg );

        T processIterable( SignatureType contentType, F arg );

        T processEnum( String id, F arg );

        T processObject( String id, F arg );
    }

    public static class MethodType
    {
        private static final long serialVersionUID = 1L;
        private final SignatureType returns;
        private final SignatureType[] parameters;

        MethodType( SignatureType returns, SignatureType... parameters )
        {
            this.returns = returns;
            this.parameters = parameters;
        }

        public SignatureType getReturnType()
        {
            return returns;
        }

        public SignatureType[] getParameterTypes()
        {
            return parameters.clone();
        }

        @Override
        public String toString()
        {
            StringBuilder result = new StringBuilder( "(" );
            for ( SignatureType parameter : parameters )
            {
                result.append( parameter );
            }
            result.append( ")" );
            result.append( returns );
            return result.toString();
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj instanceof MethodType )
            {
                MethodType method = ( MethodType ) obj;
                return method.returns.equals( this.returns )
                    && Arrays.equals( method.parameters, this.parameters );
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return returns.hashCode();
        }
    }

    public static SignatureType implicitCallbackType( String id )
    {
        return new ObjectType( id );
    }

    public static SignatureType explicitCallbackType( String id )
    {
        return new ObjectType( id );
    }

    public static SignatureType objectType( String id )
    {
        return new ObjectType( id );
    }

    public static SignatureType structType( String id )
    {
        return new ObjectType( id );
    }

    public static MethodType serviceSignature( Method method )
    {
        return getSignature( method, Context.SERVICE_PARAMETER,
            Context.SERVICE_RETURN );
    }

    public static MethodType parseServiceSignature( String signature )
    {
        return parseMethod( signature, Context.SERVICE_PARAMETER,
            Context.SERVICE_RETURN );
    }

    public static MethodType objectSignature( Method method )
    {
        return getSignature( method, Context.OBJECT_PARAMETER,
            Context.OBJECT_RETURN );
    }

    public static MethodType parseObjectSignature( String signature )
    {
        return parseMethod( signature, Context.OBJECT_PARAMETER,
            Context.OBJECT_RETURN );
    }

    public static SignatureType typeOf( Type type )
    {
        if ( type instanceof ParameterizedType )
        {
            ParameterizedType param = ( ParameterizedType ) type;
            if ( param.getRawType() == Iterable.class )
            {
                Type[] args = param.getActualTypeArguments();
                if ( args != null && args.length == 1 )
                {
                    if ( args[ 0 ] instanceof Class )
                    {
                        Class<?> cls = ( Class<?> ) args[ 0 ];
                        return new SequenceType( TypeBase.ITERABLE,
                            typeOf( cls ) );
                    }
                    else
                    {
                        throw illegal( args[ 0 ] );
                    }
                }
                return new SequenceType( TypeBase.ITERABLE, PROPERTY_VALUE );
            }
            else
            {
                throw illegal( type );
            }
        }
        else if ( type instanceof Class )
        {
            Class<?> cls = ( Class<?> ) type;
            if ( cls.isArray() )
            {
                return new SequenceType( TypeBase.ARRAY, typeOf( cls
                    .getComponentType() ) );
            }
            else
            {
                TypeBase base = primitives.get( cls );
                if ( base == null )
                {
                    // Return null to let invoker perform type lookup
                    return null;
                }
                else
                {
                    SignatureType result = basics.get( base );
                    if ( result == null )
                    {
                        throw illegal( cls );
                    }
                    return result;
                }
            }
        }
        else
        {
            throw new IllegalArgumentException( "Cannot parse type: " + type );
        }
    }

    private static SignatureType typeOf( Type type, Context context )
    {
        SignatureType result = typeOf( type );
        if ( result != null && !result.type.validIn.contains( context ) )
        {
            throw illegal( type, context );
        }
        return null;
    }

    private static MethodType getSignature( Method method,
        Context parameterContext, Context returnContext )
    {
        Type[] params = method.getGenericParameterTypes();
        SignatureType[] parameters = new SignatureType[ params.length ];
        for ( int i = 0; i < parameters.length; i++ )
        {
            parameters[ i ] = typeOf( params[ i ], parameterContext );
        }
        return new MethodType( typeOf( method.getGenericReturnType(),
            returnContext ), parameters );
    }

    private static MethodType parseMethod( String signature,
        Context parameterContext, Context returnContext )
    {
        // Trim away the method name (and flags byte).
        // Use lastIndexOf to avoid conditions where the flags byte is '(',
        // the method signature may only contain '(' once anyway.
        signature = signature.substring( signature.lastIndexOf( '(' ) );
        final int last = signature.indexOf( ')' );
        List<SignatureType> parameters = new ArrayList<SignatureType>();
        for ( int i = 1; i < last; )
        {
            i += parseParameter( parameters, signature, i, parameterContext );
        }
        SignatureType returns = parseSignature(
            signature.substring( last + 1 ), returnContext );
        return new MethodType( returns, parameters
            .toArray( new SignatureType[ 0 ] ) );
    }

    private static SignatureType parseSignature( String signature,
        Context context )
    {
        TypeBase base = bases.get( signature.charAt( 0 ) );
        if ( base == null )
        {
            throw illegal( signature );
        }
        if ( !base.validIn.contains( context ) )
        {
            throw illegal( signature, context );
        }
        SignatureType type = base.parse( signature, 0 );
        if ( type.length() != signature.length() )
        {
            throw illegal( signature );
        }
        return type;
    }

    private static int parseParameter( List<SignatureType> result,
        String signature, int start, Context context )
    {
        TypeBase base = bases.get( signature.charAt( start ) );
        if ( base == null )
        {
            throw illegal( signature );
        }
        if ( !base.validIn.contains( context ) )
        {
            throw illegal( signature, context );
        }
        SignatureType type = base.parse( signature, start );
        result.add( type );
        return type.length();
    }

    private static RuntimeException illegal( String signature )
    {
        return new IllegalArgumentException( "Illegal signature: \""
            + signature + "\"" );
    }

    private static RuntimeException illegal( String signature, Context context )
    {
        return new IllegalArgumentException( "Signature \"" + signature
            + "\" is illegal in the context of " + context );
    }

    private static RuntimeException illegal( Type type )
    {
        return new IllegalArgumentException( "Illegal type: " + type );
    }

    private static RuntimeException illegal( Type type, Context context )
    {
        return new IllegalArgumentException( "Type " + type
            + " is illigal in the context of " + context );
    }

    private static final Map<Class<?>, TypeBase> primitives;
    static
    {
        HashMap<Class<?>, TypeBase> map = new HashMap<Class<?>, TypeBase>();
        map.put( void.class, TypeBase.VOID );
        map.put( Void.class, TypeBase.VOID );
        map.put( boolean.class, TypeBase.BOOLEAN );
        map.put( Boolean.class, TypeBase.BOOLEAN );
        map.put( byte.class, TypeBase.INTEGER );
        map.put( Byte.class, TypeBase.INTEGER );
        map.put( char.class, TypeBase.INTEGER );
        map.put( Character.class, TypeBase.INTEGER );
        map.put( int.class, TypeBase.INTEGER );
        map.put( Integer.class, TypeBase.INTEGER );
        map.put( long.class, TypeBase.INTEGER );
        map.put( Long.class, TypeBase.INTEGER );
        map.put( short.class, TypeBase.INTEGER );
        map.put( Short.class, TypeBase.INTEGER );
        map.put( String.class, TypeBase.STRING );
        map.put( double.class, TypeBase.FLOAT );
        map.put( Double.class, TypeBase.FLOAT );
        map.put( float.class, TypeBase.FLOAT );
        map.put( Float.class, TypeBase.FLOAT );
        map.put( Object.class, TypeBase.PROPERTY_VALUE );
        map.put( Node.class, TypeBase.NODE );
        map.put( Relationship.class, TypeBase.RELATIONSHIP );
        map.put( RelationshipType.class, TypeBase.RELATIONSHIP_TYPE );
        primitives = Collections.unmodifiableMap( map );
    }
    private static final Map<TypeBase, SignatureType> basics;
    static
    {
        Map<TypeBase, SignatureType> map = new EnumMap<TypeBase, SignatureType>(
            TypeBase.class );
        map.put( TypeBase.VOID, VOID );
        map.put( TypeBase.BOOLEAN, BOOLEAN );
        map.put( TypeBase.INTEGER, INTEGER );
        map.put( TypeBase.STRING, STRING );
        map.put( TypeBase.NODE, NODE );
        map.put( TypeBase.RELATIONSHIP, RELATIONSHIP );
        map.put( TypeBase.RELATIONSHIP_TYPE, RELATIONSHIP_TYPE );
        map.put( TypeBase.PROPERTY_VALUE, PROPERTY_VALUE );
        basics = Collections.unmodifiableMap( map );
    }
    private static final Map<Character, TypeBase> bases;
    static
    {
        Map<Character, TypeBase> map = new HashMap<Character, TypeBase>();
        for ( TypeBase base : TypeBase.values() )
        {
            map.put( base.sig, base );
        }
        bases = Collections.unmodifiableMap( map );
    }

    private static enum TypeBase
    {
        // Simple types
        /** */
        VOID( '-', Context.RETURN ),
        /** */
        BOOLEAN( 'B', Context.ANYWHERE ),
        /** */
        INTEGER( 'I', Context.ANYWHERE ),
        /** */
        FLOAT( 'F', Context.ANYWHERE ),
        /** */
        STRING( 'S', Context.ANYWHERE ),
        /** */
        NODE( 'N', Context.ANYWHERE ),
        /** */
        RELATIONSHIP( 'R', Context.ANYWHERE ),
        /** */
        RELATIONSHIP_TYPE( 'T', Context.ANYWHERE ),
        /** */
        TRAVERSAL_POSITION( 'P', Context.ANYWHERE ),
        /** */
        PROPERTY_VALUE( 'V', Context.ANYWHERE ),
        // Complex types
        /** */
        ARRAY( '[' /* Context depends on array content */)
        {
            @Override
            SignatureType parse( String signature, int start )
            {
                return SequenceType.parse( this, signature, start );
            }
        },
        /** */
        ITERABLE( ']', Context.CALLS )
        {
            @Override
            SignatureType parse( String signature, int start )
            {
                return SequenceType.parse( this, signature, start );
            }
        },
        /** This type has no use case yet. */
        ENUM( 'E', Context.ANYWHERE )
        {
            @Override
            SignatureType parse( String signature, int start )
            {
                throw new UnsupportedOperationException(
                    "Enums are not implemented - no use case yet." );
            }
        },
        /** */
        OBJECT( '{', Context.ANYWHERE )
        {
            @Override
            SignatureType parse( String signature, int start )
            {
                return ObjectType.parse( signature, start );
            }
        },
        ;

        final char sig;
        final Set<Context> validIn; // FIXME: fix for array!

        private TypeBase( char sig, Context... validIn )
        {
            this.sig = sig;
            Set<Context> valid = EnumSet.noneOf( Context.class );
            for ( Context ctx : validIn )
            {
                valid.add( ctx );
            }
            this.validIn = Collections.unmodifiableSet( valid );
        }

        SignatureType parse( String signature, int start )
        {
            SignatureType result = basics.get( this );
            if ( result == null )
            {
                throw illegal( signature );
            }
            return result;
        }

        boolean isBasic()
        {
            return getClass() == TypeBase.class;
        }
    }
    private static enum Context
    {
        SERVICE_PARAMETER, SERVICE_RETURN, OBJECT_PARAMETER, OBJECT_RETURN,
        ARRAY_CONTENT, ITERABLE_CONTENT, ;
        static final Context[] RETURN = { SERVICE_RETURN, OBJECT_RETURN, };
        static final Context[] PARAMETER = { SERVICE_PARAMETER,
            OBJECT_PARAMETER, };
        static final Context[] CALLS = { SERVICE_PARAMETER, SERVICE_RETURN,
            OBJECT_PARAMETER, OBJECT_RETURN, };
        static final Context[] ANYWHERE = values();

        @Override
        public String toString()
        {
            return super.toString().toLowerCase().replace( '_', ' ' ) + " type";
        }
    }

    final TypeBase type;

    private SignatureType( TypeBase type )
    {
        this.type = type;
    }

    abstract int length();

    private static final class SimpleType extends SignatureType
    {
        SimpleType( TypeBase type )
        {
            super( type );
        }

        @Override
        public <F, T> T dispatch( TypeProcessor<F, T> processor, F arg )
        {
            switch ( type )
            {
                case VOID:
                    return processor.processVoid( arg );
                case BOOLEAN:
                    return processor.processBoolean( arg );
                case INTEGER:
                    return processor.processInteger( arg );
                case FLOAT:
                    return processor.processFloat( arg );
                case STRING:
                    return processor.processString( arg );
                case NODE:
                    return processor.processNode( arg );
                case RELATIONSHIP:
                    return processor.processRelationship( arg );
                case RELATIONSHIP_TYPE:
                    return processor.processRelationshipType( arg );
                case PROPERTY_VALUE:
                    return processor.processPropertyValue( arg );
                default:
                    throw new IllegalStateException( "Not a simple base type: "
                        + type );
            }
        }

        @Override
        int length()
        {
            return 1;
        }

        @Override
        public String toString()
        {
            return "" + type.sig;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj instanceof SimpleType )
            {
                SimpleType type = ( SimpleType ) obj;
                return type.type == this.type;
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return type.hashCode();
        }
    }
    private static final class SequenceType extends SignatureType
    {
        static SequenceType parse( TypeBase kind, String signature, int start )
        {
            Context context;
            switch ( kind )
            {
                case ARRAY:
                    context = Context.ARRAY_CONTENT;
                    break;
                case ITERABLE:
                    context = Context.ITERABLE_CONTENT;
                    break;
                default:
                    throw new IllegalArgumentException( "Not a sequence type: "
                        + kind );
            }
            TypeBase base = bases.get( signature.charAt( start + 1 ) );
            if ( base == null )
            {
                throw illegal( signature );
            }
            if ( !base.validIn.contains( context ) )
            {
                throw illegal( signature, context );
            }
            return new SequenceType( kind, base.parse( signature, start + 1 ) );
        }

        private final SignatureType content;

        SequenceType( TypeBase base, SignatureType content )
        {
            super( base );
            this.content = content;
        }

        @Override
        public <F, T> T dispatch( TypeProcessor<F, T> processor, F arg )
        {
            if ( type == TypeBase.ITERABLE )
            {
                return processor.processIterable( content, arg );
            }
            else
            {
                return processor.processArray( content, arg );
            }
        }

        @Override
        int length()
        {
            return content.length() + 1;
        }

        @Override
        public String toString()
        {
            return type.sig + content.toString();
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj instanceof SequenceType )
            {
                SequenceType type = ( SequenceType ) obj;
                return type.type == this.type
                    && type.content.equals( this.content );
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return content.hashCode();
        }
    }
    private static final class ObjectType extends SignatureType
    {
        static SignatureType parse( String signature, int start )
        {
            return new ObjectType( signature.substring( start + 1, signature
                .indexOf( ';', start ) ) );
        }

        private final String id;

        ObjectType( String id )
        {
            super( TypeBase.OBJECT );
            this.id = id;
        }

        @Override
        public <F, T> T dispatch( TypeProcessor<F, T> processor, F arg )
        {
            return processor.processObject( id, arg );
        }

        @Override
        int length()
        {
            return id.length() + 2;
        }

        @Override
        public String toString()
        {
            return type.sig + id + ";";
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj instanceof ObjectType )
            {
                ObjectType type = ( ObjectType ) obj;
                return type.id.equals( this.id );
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return id.hashCode();
        }
    }
}
