package org.neo4j.remote;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.remote.ServiceFactory.IndexedMethodType;
import org.neo4j.remote.ServiceFactory.TypeFactory;
import org.neo4j.remote.extension.SignatureType;
import org.neo4j.remote.extension.SignatureType.MethodType;

public abstract class ServiceSpecification implements Serializable
{
    protected ServiceSpecification()
    {
    }

    protected abstract <T> ServiceDescriptor<T> descriptor(
        Class<T> iface, RemoteNeoEngine txService );

    public static final class ObjectBuilder
    {
        private final String id;
        private final ServiceBuilder builder;

        ObjectBuilder( String id, ServiceBuilder builder )
        {
            this.id = id;
            this.builder = builder;
        }

        public ObjectBuilder addMethod( String name, MethodType signature )
        {
            builder.addObjectMethod( id, name, signature );
            return this;
        }
    }

    public static final class ServiceBuilder
    {
        private final List<String> methods = new LinkedList<String>();
        private final Map<String, String> structs = new HashMap<String, String>();
        private final Map<String, List<String>> objectMethods = new HashMap<String, List<String>>();

        public ServiceSpecification build( String identifier, int serviceId )
        {
            return new StandardServiceSpecification( identifier, serviceId,
                this );
        }

        public void addMethod( String name, MethodType signature )
        {
            methods.add( name + signature );
        }

        public void addStruct( String id, SignatureType... signatures )
        {
            StringBuilder result = new StringBuilder();
            for ( SignatureType signature : signatures )
            {
                result.append( signature );
            }
            structs.put( id, result.toString() );
        }

        public boolean hasStruct( String id )
        {
            return structs.containsKey( id );
        }

        public ObjectBuilder addObject( String id )
        {
            return new ObjectBuilder( id, this );
        }

        public boolean hasObject( String id )
        {
            return objectMethods.containsKey( id );
        }

        void addObjectMethod( String id, String name, MethodType signature )
        {
            List<String> methods = objectMethods.get( id );
            if ( methods == null )
            {
                methods = new LinkedList<String>();
                objectMethods.put( id, methods );
            }
            methods.add( name + signature );
        }

        String[] methods()
        {
            return methods.toArray( new String[ methods.size() ] );
        }

        StructSpecification[] structs()
        {
            StructSpecification[] result = new StructSpecification[ structs
                .size() ];
            int i = 0;
            for ( Map.Entry<String, String> struct : structs.entrySet() )
            {
                result[ i++ ] = new StructSpecification( struct.getKey(),
                    struct.getValue() );
            }
            return result;
        }

        ObjectSpecification[] objects()
        {
            ObjectSpecification[] result = new ObjectSpecification[ objectMethods
                .size() ];
            int i = 0;
            for ( Map.Entry<String, List<String>> object : objectMethods
                .entrySet() )
            {
                result[ i++ ] = new ObjectSpecification( object.getKey(),
                    object.getValue().toArray(
                        new String[ object.getValue().size() ] ) );
            }
            return result;
        }
    }

    private static class StructSpecification implements Serializable
    {
        private static final long serialVersionUID = 1L;
        final String id;
        final String content;

        StructSpecification( String id, String content )
        {
            this.id = id;
            this.content = content;
        }
    }
    private static class ObjectSpecification implements Serializable
    {
        private static final long serialVersionUID = 1L;
        final String id;
        final String[] methods;

        ObjectSpecification( String id, String[] methods )
        {
            this.id = id;
            this.methods = methods;
        }
    }

    private static class StandardServiceSpecification extends
        ServiceSpecification
    {
        private static final long serialVersionUID = 1L;
        private final int serviceId;
        private final String serviceIdentifier;
        private final String[] signatures;
        private final StructSpecification[] structs;
        private final ObjectSpecification[] objects;

        private StandardServiceSpecification( String identifier, int serviceId,
            ServiceBuilder builder )
        {
            this.serviceId = serviceId;
            this.serviceIdentifier = identifier;
            this.signatures = builder.methods();
            this.structs = builder.structs();
            this.objects = builder.objects();
        }

        @Override
        protected <T> ServiceDescriptor<T> descriptor(
            Class<T> iface, RemoteNeoEngine engine )
        {
            return new ServiceLoader<T>( iface, engine );
        }

        private class ServiceLoader<T> implements ServiceDescriptor<T>
        {
            private final Class<T> iface;
            private final RemoteNeoEngine engine;

            ServiceLoader( Class<T> iface, RemoteNeoEngine engine )
            {
                this.iface = iface;
                this.engine = engine;
            }

            public T getService()
            {
                ServiceFactory<T> factory = engine.getServiceFactory( iface,
                    new FactoryBuilder() );
                return factory.createServiceInstance( serviceId );
            }

            public String getIdentifier()
            {
                return serviceIdentifier;
            }
        }
        private class FactoryBuilder implements ServiceFactory.Builder
        {
            private void init()
            {
            }

            public IndexedMethodType getServiceSignature( Method method,
                TypeFactory typesFactory )
            {
                init();
                // TODO Auto-generated method stub
                return null;
            }

            public IndexedMethodType getObjectSignature( String id,
                Method method, TypeFactory typesFactory )
            {
                init();
                // TODO Auto-generated method stub
                return null;
            }
        }
    }
}
