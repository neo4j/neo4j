package org.neo4j.server.database;

import com.sun.jersey.core.spi.component.ComponentContext;
import com.sun.jersey.core.spi.component.ComponentScope;
import com.sun.jersey.server.impl.inject.AbstractHttpContextInjectable;
import com.sun.jersey.spi.inject.Injectable;
import com.sun.jersey.spi.inject.InjectableProvider;

import javax.ws.rs.core.Context;

public abstract class AbstractInjectableProvider<E>
      extends AbstractHttpContextInjectable<E>
      implements InjectableProvider<Context, Class<E>>
{
    private final Class<E> t;

    public AbstractInjectableProvider(Class<E> t) {
        this.t = t;
    }

    public Injectable<E> getInjectable( ComponentContext ic, Context a, Class<E> c) {
        if (c.equals(t)) {
            return getInjectable( );
        }

        return null;
    }

    public Injectable<E> getInjectable( ) {
        return this;
    }

    public ComponentScope getScope() {
        return ComponentScope.PerRequest;
    }
}
