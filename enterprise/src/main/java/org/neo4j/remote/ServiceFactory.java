package org.neo4j.remote;

import java.lang.reflect.Method;

import org.neo4j.remote.extension.SignatureType.MethodType;

interface ServiceFactory<T>
{
    final class IndexedMethodType
    {
        final int index;
        final MethodType signature;

        IndexedMethodType( int index, MethodType signature )
        {
            this.index = index;
            this.signature = signature;
        }
    }
    interface Builder
    {
        IndexedMethodType getServiceSignature( Method method,
            TypeFactory typesFactory );

        IndexedMethodType getObjectSignature( String id, Method method,
            TypeFactory typesFactory );
    }
    interface TypeFactory
    {
        <T extends Enum<T>> void registerEnum( String id, Class<T> type,
            String... constants );

        void registerObject( String id, Class<?> type );
    }

    T createServiceInstance( int serviceId );
}
