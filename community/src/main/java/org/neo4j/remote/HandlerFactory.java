package org.neo4j.remote;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.remote.ServiceFactory.Builder;
import org.neo4j.remote.ServiceFactory.IndexedMethodType;
import org.neo4j.remote.ServiceFactory.TypeFactory;
import org.neo4j.remote.extension.SignatureType;
import org.neo4j.remote.extension.SignatureType.MethodType;
import org.neo4j.remote.extension.SignatureType.TypeProcessor;

final class HandlerFactory
{
    private class HandlerTypeFactory implements TypeFactory,
        TypeProcessor<Void, Converter>
    {
        private final Map<SignatureType, Converter> converters = new HashMap<SignatureType, Converter>();
        private final Map<String, Converter> enums = new HashMap<String, Converter>();
        private final Map<String, Converter> objects = new HashMap<String, Converter>();
        private final Builder builder;

        public HandlerTypeFactory( Builder builder )
        {
            this.builder = builder;
        }

        Converter converter( SignatureType signature )
        {
            Converter converter = converters.get( signature );
            if ( converter == null )
            {
                converter = signature.dispatch( this, null );
            }
            return converter;
        }

        // TypeFactory implementation

        public <T extends Enum<T>> void registerEnum( String id, Class<T> type,
            String... constants )
        {
            enums.put( id, new EnumConverter( type, constants ) );
        }

        public void registerObject( String id, Class<?> iface )
        {
            Map<Method, Invocator> methods = new HashMap<Method, Invocator>();
            for ( Method method : iface.getMethods() )
            {
                IndexedMethodType type = builder.getServiceSignature( method,
                    this );
                methods.put( method, new Invocator( this, type.index,
                    type.signature ) );
            }
            objects.put( id, new ObjectConverter( iface, Collections
                .unmodifiableMap( methods ) ) );
        }

        // TypeProcessor implementation

        public Converter processVoid( Void arg )
        {
            return SimpleConverter.VOID;
        }

        public Converter processBoolean( Void arg )
        {
            return SimpleConverter.BOOLEAN;
        }

        public Converter processInteger( Void arg )
        {
            return SimpleConverter.INTEGER;
        }

        public Converter processFloat( Void arg )
        {
            return SimpleConverter.FLOAT;
        }

        public Converter processString( Void arg )
        {
            return SimpleConverter.STRING;
        }

        public Converter processNode( Void arg )
        {
            return SimpleConverter.NODE;
        }

        public Converter processRelationship( Void arg )
        {
            return SimpleConverter.RELATIONSHIP;
        }

        public Converter processRelationshipType( Void arg )
        {
            return SimpleConverter.RELATIONSHIP_TYPE;
        }

        public Converter processPropertyValue( Void arg )
        {
            return SimpleConverter.PROPERTY_VALUE;
        }

        public Converter processArray( SignatureType contentType, Void arg )
        {
            return new ArrayConverter( converter( contentType ) );
        }

        public Converter processIterable( SignatureType contentType, Void arg )
        {
            return new IterableConverter( converter( contentType ) );
        }

        public Converter processEnum( String id, Void arg )
        {
            return enums.get( id );
        }

        public Converter processObject( String id, Void arg )
        {
            return objects.get( id );
        }
    }

    private final RemoteNeoEngine engine;
    private final Map<Method, Invocator> methods;

    HandlerFactory( RemoteNeoEngine engine, Class<?> iface, Builder builder )
    {
        this.engine = engine;
        HandlerTypeFactory typesFactory = new HandlerTypeFactory( builder );
        Map<Method, Invocator> methods = new HashMap<Method, Invocator>();
        for ( Method method : iface.getMethods() )
        {
            IndexedMethodType type = builder.getServiceSignature( method,
                typesFactory );
            methods.put( method, new Invocator( typesFactory, type.index,
                type.signature ) );
        }
        this.methods = Collections.unmodifiableMap( methods );
    }

    private class Invocator
    {
        private final int functionIndex;
        private final Converter[] parameters;
        private final Converter returns;

        Invocator( HandlerTypeFactory typesFactory, int index,
            MethodType signature )
        {
            this.functionIndex = index;
            SignatureType[] signatures = signature.getParameterTypes();
            this.parameters = new Converter[ signatures.length ];
            for ( int i = 0; i < parameters.length; i++ )
            {
                parameters[ i ] = typesFactory.converter( signatures[ i ] );
            }
            this.returns = typesFactory.converter( signature.getReturnType() );
        }

        private EncodedObject[] encode( CallbackManager manager, Object[] args )
        {
            EncodedObject[] result = new EncodedObject[ args.length ];
            for ( int i = 0; i < args.length; i++ )
            {
                result[ i ] = parameters[ i ].encode( args[ i ], manager );
            }
            return result;
        }

        Object invokeServiceMethod( int serviceId, Object[] args )
        {
            CallbackManager manager = new CallbackManager();
            return returns.decode( engine.invokeServiceMethod( manager,
                serviceId, functionIndex, encode( manager, args ) ) );
        }

        Object invokeObjectMethod( int serviceId, int objectId, Object[] args )
        {
            CallbackManager manager = new CallbackManager();
            return returns.decode( engine.invokeObjectMethod( manager,
                serviceId, objectId, functionIndex, encode( manager, args ) ) );
        }
    }

    InvocationHandler makeServiceHandler( final int serviceId )
    {
        return new InvocationHandler()
        {
            public Object invoke( Object proxy, Method method, Object[] args )
                throws Throwable
            {
                return methods.get( method ).invokeServiceMethod( serviceId,
                    args );
            }
        };
    }

    private Object makeObject( Class<?> iface, int serviceId, int objectId,
        Map<Method, Invocator> methods )
    {
        return Proxy.newProxyInstance( iface.getClassLoader(),
            new Class[] { iface }, new ObjectHandler( serviceId, objectId,
                methods ) );
    }

    private class ObjectHandler implements InvocationHandler
    {
        private final int serviceId;
        final int objectId;
        private final Map<Method, Invocator> methods;

        ObjectHandler( int serviceId, int objectId,
            Map<Method, Invocator> methods )
        {
            this.serviceId = serviceId;
            this.objectId = objectId;
            this.methods = methods;
        }

        @Override
        protected void finalize() throws Throwable
        {
            engine.finalizeObject( serviceId, objectId );
        }

        public Object invoke( Object proxy, Method method, Object[] args )
            throws Throwable
        {
            return methods.get( method ).invokeObjectMethod( serviceId,
                objectId, args );
        }
    }

    private static interface Converter
    {
        EncodedObject encode( Object object, CallbackManager manager );

        Object decode( EncodedObject object );
    }
    private static enum SimpleConverter implements Converter
    {
        VOID
        {
            @Override
            public Object decode( EncodedObject object )
            {
                return null;
            }

            @Override
            public EncodedObject encode( Object object, CallbackManager manager )
            {
                return null;
            }
        },
        BOOLEAN
        {
            @Override
            public Object decode( EncodedObject object )
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public EncodedObject encode( Object object, CallbackManager manager )
            {
                // TODO Auto-generated method stub
                return null;
            }
        },
        INTEGER
        {
            @Override
            public Object decode( EncodedObject object )
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public EncodedObject encode( Object object, CallbackManager manager )
            {
                // TODO Auto-generated method stub
                return null;
            }
        },
        FLOAT
        {
            @Override
            public Object decode( EncodedObject object )
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public EncodedObject encode( Object object, CallbackManager manager )
            {
                // TODO Auto-generated method stub
                return null;
            }
        },
        STRING
        {
            @Override
            public Object decode( EncodedObject object )
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public EncodedObject encode( Object object, CallbackManager manager )
            {
                // TODO Auto-generated method stub
                return null;
            }
        },
        NODE
        {
            @Override
            public Object decode( EncodedObject object )
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public EncodedObject encode( Object object, CallbackManager manager )
            {
                // TODO Auto-generated method stub
                return null;
            }
        },
        RELATIONSHIP
        {
            @Override
            public Object decode( EncodedObject object )
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public EncodedObject encode( Object object, CallbackManager manager )
            {
                // TODO Auto-generated method stub
                return null;
            }
        },
        RELATIONSHIP_TYPE
        {
            @Override
            public Object decode( EncodedObject object )
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public EncodedObject encode( Object object, CallbackManager manager )
            {
                // TODO Auto-generated method stub
                return null;
            }
        },
        PROPERTY_VALUE
        {
            @Override
            public Object decode( EncodedObject object )
            {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public EncodedObject encode( Object object, CallbackManager manager )
            {
                // TODO Auto-generated method stub
                return null;
            }
        };

        public abstract Object decode( EncodedObject object );

        public abstract EncodedObject encode( Object object,
            CallbackManager manager );
    }
    private static class ArrayConverter implements Converter
    {
        final Converter content;

        ArrayConverter( Converter content )
        {
            this.content = content;

        }

        public Object decode( EncodedObject object )
        {
            // TODO Auto-generated method stub
            return null;
        }

        public EncodedObject encode( Object object, CallbackManager manager )
        {
            if ( object instanceof Object[] )
            {
                Object[] array = ( Object[] ) object;

            }
            else if ( object instanceof Collection )
            {
                Collection<?> collection = ( Collection<?> ) object;

            }
            else if ( object instanceof Iterable )
            {
                Iterable<?> iterable = ( Iterable<?> ) object;

            }
            return null;
        }
    }
    private static class IterableConverter extends ArrayConverter
    {
        IterableConverter( Converter content )
        {
            super( content );
        }

        @Override
        public Object decode( EncodedObject object )
        {
            // TODO Auto-generated method stub
            return null;
        }
    }
    private static class EnumConverter implements Converter
    {
        private final Class<? extends Enum<?>> type;
        private final Map<Enum<?>, Integer> encodeTable;
        private final Object[] decodeTable;

        <T extends Enum<T>> EnumConverter( Class<T> type, String[] constants )
        {
            this.type = type;
            encodeTable = ( Map<Enum<?>, Integer> ) new EnumMap<T, Integer>(
                type );
            decodeTable = new Object[ constants.length ];
        }

        public Object decode( EncodedObject object )
        {
            // TODO Auto-generated method stub
            return null;
        }

        public EncodedObject encode( Object object, CallbackManager manager )
        {
            Enum<?> e = type.cast( object );
            int code = encodeTable.get( e );
            // TODO Auto-generated method stub
            return null;
        }
    }
    private class ObjectConverter implements Converter
    {
        private final Class<?> iface;
        private final Map<Method, Invocator> objectMethods;

        ObjectConverter( Class<?> iface, Map<Method, Invocator> methods )
        {
            this.iface = iface;
            this.objectMethods = methods;
        }

        public Object decode( EncodedObject object )
        {
            int serviceId = 0, objectId = 0;
            // TODO Auto-generated method stub
            return makeObject( iface, serviceId, objectId, objectMethods );
        }

        public EncodedObject encode( Object object, CallbackManager manager )
        {
            if ( !iface.isInstance( object ) )
            {
                throw new IllegalArgumentException(
                    "Object does not implement required interface" );
            }
            if ( Proxy.isProxyClass( object.getClass() ) )
            {
                InvocationHandler handler = Proxy.getInvocationHandler( object );
                if ( handler instanceof ObjectHandler )
                {
                    ObjectHandler internal = ( ObjectHandler ) handler;
                    // TODO: remote object representation
                    return null;
                }
            }
            // TODO: callback representation
            return null;
        }
    }
}
