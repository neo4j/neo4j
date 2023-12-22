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
package org.neo4j.server.web;

import static org.glassfish.jersey.server.ServerProperties.WADL_FEATURE_DISABLE;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.neo4j.server.bind.ComponentsBinder;
import org.neo4j.server.http.error.MediaTypeExceptionMapper;
import org.neo4j.server.modules.ServerModule;

/**
 * Different {@link ServerModule}s can register services at the same mount point.
 * So this class will collect all packages/classes per mount point and create the {@link ServletHolder}
 * when all modules have registered services, see {@link #create(ComponentsBinder, boolean)}.
 */
public class JaxRsServletHolderFactory {
    private final Set<String> packages = new HashSet<>();
    private final Set<Class<?>> classes = new HashSet<>();
    private final List<Injectable<?>> injectables = new ArrayList<>();

    public JaxRsServletHolderFactory() {
        // add classes common to all mount points
        classes.add(MediaTypeExceptionMapper.class);
        classes.add(JacksonJsonProvider.class);
    }

    public void addPackages(List<String> packages, Collection<Injectable<?>> injectableProviders) {
        this.packages.addAll(packages);
        if (injectableProviders != null) {
            this.injectables.addAll(injectableProviders);
        }
    }

    public void addClasses(List<Class<?>> classes, Collection<Injectable<?>> injectableProviders) {
        this.classes.addAll(classes);
        if (injectableProviders != null) {
            this.injectables.addAll(injectableProviders);
        }
    }

    public void removePackages(List<String> packages) {
        this.packages.removeAll(packages);
    }

    public void removeClasses(List<Class<?>> classes) {
        this.classes.removeAll(classes);
    }

    public ServletHolder create(ComponentsBinder binder, boolean wadlEnabled) {
        for (Injectable<?> injectable : injectables) {
            Supplier<?> getValue = injectable::getValue;
            Class<?> type = injectable.getType();
            binder.addLazyBinding(getValue, type);
        }

        ResourceConfig resourceConfig = new ResourceConfig()
                .register(binder)
                .register(XForwardFilter.class)
                .packages(packages.toArray(new String[0]))
                .registerClasses(classes)
                .property(WADL_FEATURE_DISABLE, String.valueOf(!wadlEnabled));

        ServletContainer container = new ServletContainer(resourceConfig);
        return new ServletHolder(container);
    }
}
