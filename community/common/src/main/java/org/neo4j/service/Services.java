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
package org.neo4j.service;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.neo4j.util.FeatureToggles.flag;
import static org.neo4j.util.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.function.Function;

/**
 * Utilities to load services via {@link ServiceLoader}.
 */
public final class Services {
    private static final boolean PRINT_SERVICE_LOADER_STACK_TRACES =
            flag(Services.class, "printServiceLoaderStackTraces", false);
    private static final boolean THROW_SERVICE_LOADER_EXCEPTIONS =
            flag(Services.class, "throwServiceLoaderExceptions", false);

    private Services() {
        // util class
    }

    /**
     * Load all available implementations of a service, if any.
     *
     * @param service the type of the service.
     * @return all registered implementations of the SPI.
     */
    public static <T> Collection<T> loadAll(Class<T> service) {
        return loadAll(Services.class.getClassLoader(), service);
    }

    /**
     * Load all available implementations of a service, if any.
     *
     * @param classLoader the classloader to search from.
     * @param service the type of the service.
     * @return all registered implementations of the SPI.
     */
    public static <T> Collection<T> loadAll(ClassLoader classLoader, Class<T> service) {
        final Map<String, T> providers = new HashMap<>();
        final ClassLoader contextCL = Thread.currentThread().getContextClassLoader();

        loadAllSafely(service, contextCL)
                .forEach(provider -> providers.put(provider.getClass().getName(), provider));

        // in application servers, osgi and alike environments, context classloader can differ from the one that loads
        // neo4j libs;
        // in such cases we need to load services from both
        if (classLoader != contextCL) {
            // services from context class loader have higher precedence, so we skip duplicates by comparing class
            // names.
            loadAllSafely(service, classLoader)
                    .forEach(provider ->
                            providers.putIfAbsent(provider.getClass().getName(), provider));
        }

        return new ArrayList<>(providers.values());
    }

    /**
     * Load service provider with the specified name.
     *
     * @throws RuntimeException if multiple service providers with the same name are found
     */
    public static <T extends NamedService> Optional<T> load(Class<T> service, String name) {
        checkArgument(isNotBlank(name), "Service provider name is null or blank");
        return load(service, name, NamedService::getName);
    }

    /**
     * Load service provider with the specified key.
     *
     * @throws RuntimeException if multiple service providers with the same key are found
     */
    public static <T, K> Optional<T> load(Class<T> service, K key, Function<T, K> keyAccessor) {
        requireNonNull(key, "Service provider key is null");
        final List<T> matches = loadAll(service).stream()
                .filter(provider -> key.equals(keyAccessor.apply(provider)))
                .toList();

        if (matches.size() > 1) {
            throw new RuntimeException(format("Found multiple service providers %s[%s]: %s", service, key, matches));
        }

        return matches.isEmpty() ? Optional.empty() : Optional.of(matches.get(0));
    }

    /**
     * Load the service with the highest priority.
     */
    public static <T extends PrioritizedService> Optional<T> loadByPriority(Class<T> service) {
        final List<T> all = (List<T>) loadAll(service);
        all.sort(Comparator.comparingInt(PrioritizedService::getPriority));
        return all.isEmpty() ? Optional.empty() : Optional.of(all.get(0));
    }

    /**
     * Load service provider implementation with the specified name, or fail if there's no match.
     *
     * @throws NoSuchElementException if no service could be loaded with the given name
     */
    public static <T extends NamedService> T loadOrFail(Class<T> service, String name) {
        return loadOrFail(service, name, NamedService::getName);
    }

    /**
     * Load service provider implementation with the specified key, or fail if there's no match.
     *
     * @throws NoSuchElementException if no service could be loaded with the given key
     */
    public static <T, K> T loadOrFail(Class<T> service, K key, Function<T, K> keyAccessor) {
        return load(service, key, keyAccessor)
                .orElseThrow(() -> new NoSuchElementException(
                        format("Could not find service provider %s[%s]", service.getName(), key)));
    }

    private static <T> List<T> loadAllSafely(Class<T> type, ClassLoader classLoader) {
        final List<T> services = new ArrayList<>();
        final Iterator<T> loader = ServiceLoader.load(type, classLoader).iterator();
        while (loader.hasNext()) {
            try {
                services.add(loader.next());
            } catch (ServiceConfigurationError e) {
                if (THROW_SERVICE_LOADER_EXCEPTIONS) {
                    throw e;
                }
                if (PRINT_SERVICE_LOADER_STACK_TRACES) {
                    e.printStackTrace();
                }
            }
        }
        return services;
    }
}
