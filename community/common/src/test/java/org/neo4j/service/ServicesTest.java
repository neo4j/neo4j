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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.neo4j.service.test.BarService;
import org.neo4j.service.test.BazService;
import org.neo4j.service.test.FooService;
import org.neo4j.service.test.NotNamedService;
import org.neo4j.service.test.ServiceWithDuplicateName;
import org.neo4j.service.test.SomeService;

class ServicesTest {

    @Test
    void loadAll() {
        final Collection<SomeService> services = Services.loadAll(SomeService.class);
        assertThat(services)
                .hasSize(2)
                .hasAtLeastOneElementOfType(FooService.class)
                .hasAtLeastOneElementOfType(BarService.class);
    }

    @Test
    void loadByName() {
        final Optional<SomeService> foo = Services.load(SomeService.class, "foo");
        assertTrue(foo.isPresent());
        assertThat(foo.get()).isInstanceOf(FooService.class);

        final Optional<SomeService> bar = Services.load(SomeService.class, "bar");
        assertTrue(bar.isPresent());
        assertThat(bar.get()).isInstanceOf(BarService.class);
    }

    @Test
    void loadByKey() {
        final Optional<NotNamedService> impl1 = Services.load(NotNamedService.class, 1L, NotNamedService::id);
        assertTrue(impl1.isPresent());
        assertThat(impl1.get()).isInstanceOf(NotNamedService.ServiceImpl1.class);

        final Optional<NotNamedService> impl2 = Services.load(NotNamedService.class, 2L, NotNamedService::id);
        assertTrue(impl2.isPresent());
        assertThat(impl2.get()).isInstanceOf(NotNamedService.ServiceImpl2.class);
    }

    @Test
    void loadOrFail_load() {
        final SomeService service = Services.loadOrFail(SomeService.class, "foo");
        assertThat(service).isInstanceOf(FooService.class);
    }

    @Test
    void loadOrFail_fail() {
        assertThrows(NoSuchElementException.class, () -> Services.loadOrFail(SomeService.class, "nonexisting-key"));
    }

    @Test
    void loadByNameNoMatch() {
        final Optional<SomeService> provider = Services.load(SomeService.class, "nonexisting-key");
        assertFalse(provider.isPresent());
    }

    @Test
    void failOnDuplicateKeyWhenLoadingByName() {
        final RuntimeException error = assertThrows(
                RuntimeException.class, () -> Services.load(ServiceWithDuplicateName.class, "duplicate-name"));
        assertThat(error.getMessage()).contains("Found multiple service providers");
    }

    @Test
    void loadByPriority() {
        final Optional<SomeService> foo = Services.loadByPriority(SomeService.class);
        assertTrue(foo.isPresent());
        assertThat(foo.get()).isInstanceOf(FooService.class);
    }

    @Test
    void loadAllFromCurrentAndContextClassLoadersNoDuplicates() {
        withContextClassLoader(new TestClassLoader(), () -> {
            final Collection<SomeService> services = Services.loadAll(SomeService.class);
            assertThat(services)
                    .hasSize(3)
                    .hasAtLeastOneElementOfType(FooService.class)
                    .hasAtLeastOneElementOfType(BarService.class)
                    .hasAtLeastOneElementOfType(BazService.class);
        });
    }

    @Test
    void loadByNameFromCurrentAndContextClassLoaders() {
        withContextClassLoader(new TestClassLoader(), () -> {
            final Optional<SomeService> foo = Services.load(SomeService.class, "foo");
            assertTrue(foo.isPresent());
            assertThat(foo.get()).isInstanceOf(FooService.class);

            final Optional<SomeService> baz = Services.load(SomeService.class, "baz");
            assertTrue(baz.isPresent());
            assertThat(baz.get()).isInstanceOf(BazService.class);
        });
    }

    @Test
    void loadByPriorityFromCurrentAndContextClassLoaders() {
        withContextClassLoader(new TestClassLoader(), () -> {
            final Optional<SomeService> foo = Services.loadByPriority(SomeService.class);
            assertTrue(foo.isPresent());
            assertThat(foo.get()).isInstanceOf(FooService.class);
        });
    }

    @Test
    void skipsAProviderThisRuntimeCannotLink() {
        // The first of the two the test services file lists, so that loading has to carry on past it.
        var loader = new TooNewClassLoader(FooService.class.getName());
        assertThat(Services.loadAllSafely(SomeService.class, loader, false))
                .singleElement()
                .isInstanceOf(BazService.class);
    }

    @Test
    void throwsAProviderThisRuntimeCannotLinkWhenToldTo() {
        var loader = new TooNewClassLoader(FooService.class.getName());
        assertThrows(UnsupportedClassVersionError.class, () -> Services.loadAllSafely(SomeService.class, loader, true));
    }

    private static void withContextClassLoader(ClassLoader contextClassLoader, Runnable action) {
        final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
            action.run();
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    private static final class TooNewClassLoader extends ClassLoader {
        private final String tooNew;
        private final TestClassLoader services = new TestClassLoader();

        TooNewClassLoader(String tooNew) {
            this.tooNew = tooNew;
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!name.equals(tooNew)) {
                return super.loadClass(name, resolve);
            }
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded != null) {
                    return loaded;
                }
                try (InputStream in = getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
                    byte[] bytes = in.readAllBytes();
                    // Bump major version of the class file, it's an unsigned 16-bit integer at offset 6 and 7
                    // ClassFile {
                    //    u4             magic;          // 0-3
                    //    u2             minor_version;  // 4-5
                    //    u2             major_version;  // 6-7
                    //    ...
                    // }
                    // While not pretty, no other way of faking this was found.
                    bytes[7] += 40; // 20+ years with the current release cadence
                    return defineClass(name, bytes, 0, bytes.length);
                } catch (IOException e) {
                    throw new ClassNotFoundException(name, e);
                }
            }
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException {
            return services.getResources(name);
        }

        @Override
        public URL getResource(String name) {
            return services.getResource(name);
        }
    }

    private static final class TestClassLoader extends ClassLoader {
        @Override
        public URL getResource(String name) {
            return name.startsWith("META-INF/services") ? super.getResource("test/" + name) : super.getResource(name);
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException {
            return name.startsWith("META-INF/services") ? super.getResources("test/" + name) : super.getResources(name);
        }

        @Override
        public InputStream getResourceAsStream(String name) {
            return name.startsWith("META-INF/services")
                    ? super.getResourceAsStream("test/" + name)
                    : super.getResourceAsStream(name);
        }
    }
}
