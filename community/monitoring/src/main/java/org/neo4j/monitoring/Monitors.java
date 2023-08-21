/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.monitoring;

import static org.apache.commons.lang3.ArrayUtils.isEmpty;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.apache.commons.lang3.ClassUtils;
import org.eclipse.collections.api.bag.MutableBag;
import org.eclipse.collections.impl.bag.mutable.MultiReaderHashBag;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

/**
 * This can be used to create monitor instances using a Dynamic Proxy, which when invoked can delegate to any number of
 * listeners. Listeners also implement the monitor interface.
 *
 * The creation of monitors and registration of listeners may happen in any order. Listeners can be registered before
 * creating the actual monitor, and vice versa.
 *
 * Components that actually implement listening functionality must be registered using {{@link #addMonitorListener(Object, String...)}.
 *
 * This class is thread-safe.
 */
public class Monitors {
    private static final FailureHandler IGNORE = (f, m) -> {};

    /** Monitor interface method -> Listeners */
    private final Map<Method, Set<MonitorListenerInvocationHandler>> methodMonitorListeners = new ConcurrentHashMap<>();

    private final MutableBag<Class<?>> monitoredInterfaces = MultiReaderHashBag.newBag();
    private final Monitors parent;
    private final FailureHandler failureHandler;

    public Monitors() {
        this(null, IGNORE);
    }

    /**
     * @param failureHandler will be called if an exception is thrown from one of the listeners.
     */
    public Monitors(FailureHandler failureHandler) {
        this(null, failureHandler);
    }

    /**
     * Create a child Monitors instance with a given {@code parent}.
     * Events are propagated up, so any listeners connected to the parent instance will see events from monitor objects
     * {@link #newMonitor(Class, String...) created} on this instance.
     * Listeners registered on this Monitors instance will receive events from monitor objects {@link #newMonitor(Class, String...) created} on this
     * instance or any of its children - they will NOT see events on monitor objects connected to the parent instance.
     * <p>
     * Events from monitor objects will bubble up from the children in a way that listeners on the child monitor will be invoked before the
     * parent ones.
     *
     * @param parent to propagate events to.
     * @param logProvider to create a logger.
     */
    public Monitors(Monitors parent, InternalLogProvider logProvider) {
        this(parent, new LoggingFailureHandler(logProvider));
    }

    public Monitors(Monitors parent, FailureHandler failureHandler) {
        this.parent = parent;
        this.failureHandler = failureHandler;
    }

    public <T> T newMonitor(Class<T> monitorClass, String... tags) {
        requireInterface(monitorClass);
        ClassLoader classLoader = monitorClass.getClassLoader();
        MonitorInvocationHandler monitorInvocationHandler = new MonitorInvocationHandler(this, tags);
        return monitorClass.cast(
                Proxy.newProxyInstance(classLoader, new Class<?>[] {monitorClass}, monitorInvocationHandler));
    }

    public void addMonitorListener(Object monitorListener, String... tags) {
        MonitorListenerInvocationHandler monitorListenerInvocationHandler =
                createInvocationHandler(monitorListener, tags);

        List<Class<?>> listenerInterfaces = getAllInterfaces(monitorListener);
        methodsStream(listenerInterfaces).forEach(method -> {
            Set<MonitorListenerInvocationHandler> methodHandlers =
                    methodMonitorListeners.computeIfAbsent(method, f -> ConcurrentHashMap.newKeySet());
            methodHandlers.add(monitorListenerInvocationHandler);
        });
        monitoredInterfaces.addAll(listenerInterfaces);
    }

    public void removeMonitorListener(Object monitorListener) {
        List<Class<?>> listenerInterfaces = getAllInterfaces(monitorListener);
        methodsStream(listenerInterfaces).forEach(method -> cleanupMonitorListeners(monitorListener, method));
        listenerInterfaces.forEach(monitoredInterfaces::remove);
    }

    private void cleanupMonitorListeners(Object monitorListener, Method key) {
        methodMonitorListeners.computeIfPresent(key, (method1, handlers) -> {
            handlers.removeIf(handler -> monitorListener.equals(handler.getMonitorListener()));
            return handlers.isEmpty() ? null : handlers;
        });
    }

    private static List<Class<?>> getAllInterfaces(Object monitorListener) {
        return ClassUtils.getAllInterfaces(monitorListener.getClass());
    }

    private static Stream<Method> methodsStream(List<Class<?>> interfaces) {
        return interfaces.stream().map(Class::getMethods).flatMap(Arrays::stream);
    }

    private static MonitorListenerInvocationHandler createInvocationHandler(Object monitorListener, String[] tags) {
        return isEmpty(tags)
                ? new UntaggedMonitorListenerInvocationHandler(monitorListener)
                : new TaggedMonitorListenerInvocationHandler(monitorListener, tags);
    }

    private static void requireInterface(Class monitorClass) {
        if (!monitorClass.isInterface()) {
            throw new IllegalArgumentException("Interfaces should be provided.");
        }
    }

    private interface MonitorListenerInvocationHandler {
        Object getMonitorListener();

        void invoke(Object proxy, Method method, Object[] args, String... tags) throws Throwable;
    }

    private static class UntaggedMonitorListenerInvocationHandler implements MonitorListenerInvocationHandler {
        private final Object monitorListener;

        UntaggedMonitorListenerInvocationHandler(Object monitorListener) {
            this.monitorListener = monitorListener;
        }

        @Override
        public Object getMonitorListener() {
            return monitorListener;
        }

        @Override
        public void invoke(Object proxy, Method method, Object[] args, String... tags) throws Throwable {
            method.invoke(monitorListener, args);
        }
    }

    private static class TaggedMonitorListenerInvocationHandler extends UntaggedMonitorListenerInvocationHandler {
        private final String[] tags;

        TaggedMonitorListenerInvocationHandler(Object monitorListener, String... tags) {
            super(monitorListener);
            this.tags = tags;
        }

        @Override
        public void invoke(Object proxy, Method method, Object[] args, String... tags) throws Throwable {
            if (ArrayUtil.containsAll(this.tags, tags)) {
                super.invoke(proxy, method, args, tags);
            }
        }
    }

    private static class MonitorInvocationHandler implements InvocationHandler {
        private final Monitors monitor;
        private final String[] tags;

        MonitorInvocationHandler(Monitors monitor, String... tags) {
            this.monitor = monitor;
            this.tags = tags;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            invokeMonitorListeners(monitor, tags, proxy, method, args);

            // Bubble up
            Monitors current = monitor.parent;
            while (current != null) {
                invokeMonitorListeners(current, tags, proxy, method, args);
                current = current.parent;
            }
            return null;
        }

        private static void invokeMonitorListeners(
                Monitors monitor, String[] tags, Object proxy, Method method, Object[] args) {
            Set<MonitorListenerInvocationHandler> handlers = monitor.methodMonitorListeners.get(method);
            if (handlers == null || handlers.isEmpty()) {
                return;
            }
            for (MonitorListenerInvocationHandler monitorListenerInvocationHandler : handlers) {
                try {
                    monitorListenerInvocationHandler.invoke(proxy, method, args, tags);
                } catch (Throwable failure) {
                    monitor.failureHandler.accept(failure, method.getName());
                }
            }
        }
    }

    public interface FailureHandler {
        void accept(Throwable failure, String method);
    }

    public static class LoggingFailureHandler implements FailureHandler {
        private final InternalLog log;

        public LoggingFailureHandler(InternalLogProvider logProvider) {
            log = logProvider.getLog(Monitors.class);
        }

        @Override
        public void accept(Throwable failure, String method) {
            String message =
                    String.format("Encountered exception while handling listener for monitor method %s", method);
            log.warn(message, failure);
        }
    }
}
