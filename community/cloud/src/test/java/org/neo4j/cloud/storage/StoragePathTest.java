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
package org.neo4j.cloud.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.cloud.storage.PathRepresentation.EMPTY_PATH;

import java.nio.file.WatchEvent.Modifier;
import java.util.Iterator;
import java.util.List;
import org.eclipse.collections.api.factory.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StoragePathTest {

    private static final String SCHEME = "testing";

    private final StorageSystem system = mock(StorageSystem.class);
    private final StorageSystem otherSystem = mock(StorageSystem.class);
    private final StorageSystem altSystem = mock(StorageSystem.class);

    @BeforeEach
    void setup() {
        when(system.scheme()).thenReturn(SCHEME);
        when(system.uriPrefix()).thenReturn(SCHEME + "://bucket1");
        when(system.canResolve(any()))
                .thenAnswer(call -> ((StoragePath) call.getArgument(0)).getFileSystem() == system);

        when(otherSystem.scheme()).thenReturn(SCHEME);
        when(otherSystem.uriPrefix()).thenReturn(SCHEME + "://bucket2");
        when(otherSystem.canResolve(any()))
                .thenAnswer(call -> ((StoragePath) call.getArgument(0)).getFileSystem() == otherSystem);

        when(altSystem.scheme()).thenReturn("alt");
        when(altSystem.uriPrefix()).thenReturn(SCHEME + "://");
        when(altSystem.canResolve(any()))
                .thenAnswer(call -> ((StoragePath) call.getArgument(0)).getFileSystem() == altSystem);
    }

    @Test
    void isRoot() {
        assertThat(StoragePath.isRoot(path("/"))).isTrue();
        assertThat(StoragePath.isRoot(path(EMPTY_PATH))).isFalse();
        assertThat(StoragePath.isRoot(path("/foo"))).isFalse();
        assertThat(StoragePath.isRoot(path("foo"))).isFalse();
    }

    @Test
    void isEmpty() {
        assertThat(StoragePath.isEmpty(path(EMPTY_PATH))).isTrue();
        assertThat(StoragePath.isEmpty(path("/"))).isFalse();
        assertThat(StoragePath.isEmpty(path("/foo"))).isFalse();
        assertThat(StoragePath.isEmpty(path("foo"))).isFalse();
    }

    @Test
    void isAbsolute() {
        assertThat(path("/").isAbsolute()).isTrue();
        assertThat(path("/foo").isAbsolute()).isTrue();
        assertThat(path("/foo/bar").isAbsolute()).isTrue();

        assertThat(path(EMPTY_PATH).isAbsolute()).isFalse();
        assertThat(path("foo").isAbsolute()).isFalse();
        assertThat(path("foo/bar").isAbsolute()).isFalse();
    }

    @Test
    void isDirectory() {
        assertThat(path(EMPTY_PATH).isDirectory()).isTrue();
        assertThat(path("/").isDirectory()).isTrue();
        assertThat(path("/foo/").isDirectory()).isTrue();
        assertThat(path("/foo/bar/").isDirectory()).isTrue();
        assertThat(path("foo/").isDirectory()).isTrue();
        assertThat(path("foo/bar/").isDirectory()).isTrue();
        assertThat(path("foo/bar/.").isDirectory()).isTrue();
        assertThat(path("foo/bar/..").isDirectory()).isTrue();

        assertThat(path("/foo").isDirectory()).isFalse();
        assertThat(path("/foo/bar").isDirectory()).isFalse();
        assertThat(path("foo").isDirectory()).isFalse();
        assertThat(path("foo/bar").isDirectory()).isFalse();
    }

    @Test
    void getRoot() {
        final var root = path("/");
        assertThat(root.getRoot()).isEqualTo(root);
        assertThat(path("/foo").getRoot()).isEqualTo(root);
        assertThat(path("/foo/").getRoot()).isEqualTo(root);
        assertThat(path("/foo/bar").getRoot()).isEqualTo(root);
        assertThat(path("/foo/bar/").getRoot()).isEqualTo(root);

        assertThat(path(EMPTY_PATH).getRoot()).isNull();
        assertThat(path("foo").getRoot()).isNull();
        assertThat(path("foo/").getRoot()).isNull();
        assertThat(path("foo/bar").getRoot()).isNull();
    }

    @Test
    void getFileName() {
        assertThat(path("/").getFileName()).isNull();
        assertThat(path(EMPTY_PATH).getFileName()).isNull();
        assertThat(path("/foo").getFileName().toString()).isEqualTo("foo");
        assertThat(path("/foo/").getFileName().toString()).isEqualTo("foo/");
        assertThat(path("/foo/bar").getFileName().toString()).isEqualTo("bar");
        assertThat(path("/foo/bar/").getFileName().toString()).isEqualTo("bar/");
        assertThat(path("foo").getFileName().toString()).isEqualTo("foo");
        assertThat(path("foo/").getFileName().toString()).isEqualTo("foo/");
        assertThat(path("foo/bar").getFileName().toString()).isEqualTo("bar");
        assertThat(path("foo/bar/").getFileName().toString()).isEqualTo("bar/");
    }

    @Test
    void getNameCount() {
        assertThat(path("/").getNameCount()).isZero();
        assertThat(path(EMPTY_PATH).getNameCount()).isZero();
        assertThat(path("/foo").getNameCount()).isOne();
        assertThat(path("/foo/").getNameCount()).isOne();
        assertThat(path("foo").getNameCount()).isOne();
        assertThat(path("foo/").getNameCount()).isOne();
        assertThat(path("/foo/bar").getNameCount()).isEqualTo(2);
        assertThat(path("/foo/bar/").getNameCount()).isEqualTo(2);
        assertThat(path("foo/bar").getNameCount()).isEqualTo(2);
        assertThat(path("foo/bar/").getNameCount()).isEqualTo(2);
        assertThat(path("/foo/bar/baz").getNameCount()).isEqualTo(3);
        assertThat(path("/foo/bar/baz/").getNameCount()).isEqualTo(3);
        assertThat(path("foo/bar/baz").getNameCount()).isEqualTo(3);
        assertThat(path("foo/bar/baz/").getNameCount()).isEqualTo(3);
    }

    @Test
    void getName() {
        final var foo = path("foo");
        final var fooSlash = path("foo/");
        final var bar = path("bar");
        final var barSlash = path("bar/");
        final var baz = path("baz");
        final var bazSlash = path("baz/");

        assertThat(path("/foo").getName(0)).isEqualTo(foo);
        assertThat(path("/foo/").getName(0)).isEqualTo(fooSlash);
        assertThat(path("foo").getName(0)).isEqualTo(foo);
        assertThat(path("foo/").getName(0)).isEqualTo(fooSlash);
        assertThat(path("/foo/bar").getName(1)).isEqualTo(bar);
        assertThat(path("/foo/bar/").getName(1)).isEqualTo(barSlash);
        assertThat(path("foo/bar").getName(1)).isEqualTo(bar);
        assertThat(path("foo/bar/").getName(1)).isEqualTo(barSlash);
        assertThat(path("/foo/bar/baz").getName(1)).isEqualTo(barSlash);
        assertThat(path("/foo/bar/baz/").getName(1)).isEqualTo(barSlash);
        assertThat(path("foo/bar/baz").getName(1)).isEqualTo(barSlash);
        assertThat(path("foo/bar/baz/").getName(1)).isEqualTo(barSlash);
        assertThat(path("/foo/bar/baz").getName(2)).isEqualTo(baz);
        assertThat(path("/foo/bar/baz/").getName(2)).isEqualTo(bazSlash);
        assertThat(path("foo/bar/baz").getName(2)).isEqualTo(baz);
        assertThat(path("foo/bar/baz/").getName(2)).isEqualTo(bazSlash);
    }

    @Test
    void getParent() {
        final var root = path("/");
        final var foo = path("/foo/");
        final var bar = foo.resolve("bar/");
        final var baz = bar.resolve("baz");
        final var bof = bar.resolve("bof/");

        final var fooR = path("foo/");
        final var barR = fooR.resolve("bar/");
        final var bazR = barR.resolve("baz");
        final var bofR = barR.resolve("bof/");

        assertThat(baz.getParent()).isEqualTo(bar);
        assertThat(bof.getParent()).isEqualTo(bar);
        assertThat(bar.getParent()).isEqualTo(foo);
        assertThat(foo.getParent()).isEqualTo(root);
        assertThat(root.getParent()).isNull();

        assertThat(bazR.getParent()).isEqualTo(barR);
        assertThat(bofR.getParent()).isEqualTo(barR);
        assertThat(barR.getParent()).isEqualTo(fooR);
        assertThat(fooR.getParent()).isNull();
    }

    @Test
    void subPath() {
        final var path1 = path("/foo/bar/baz");
        final var path2 = path("/foo/bar/baz/");

        assertThat(path1.subpath(0, 1)).isEqualTo(path("foo/"));
        assertThat(path1.subpath(0, 2)).isEqualTo(path("foo/bar/"));
        assertThat(path1.subpath(0, 3)).isEqualTo(path("foo/bar/baz"));
        assertThat(path1.subpath(1, 2)).isEqualTo(path("bar/"));
        assertThat(path1.subpath(1, 3)).isEqualTo(path("bar/baz"));
        assertThat(path1.subpath(2, 3)).isEqualTo(path("baz"));

        assertThat(path2.subpath(0, 1)).isEqualTo(path("foo/"));
        assertThat(path2.subpath(0, 2)).isEqualTo(path("foo/bar/"));
        assertThat(path2.subpath(0, 3)).isEqualTo(path("foo/bar/baz/"));
        assertThat(path2.subpath(1, 2)).isEqualTo(path("bar/"));
        assertThat(path2.subpath(1, 3)).isEqualTo(path("bar/baz/"));
        assertThat(path2.subpath(2, 3)).isEqualTo(path("baz/"));
    }

    @Test
    void startsWith() {
        final var path1 = path("/foo/bar/baz");
        final var path2 = path("/foo/bar/baz/");
        final var path3 = path("foo/bar/baz/");

        assertThat(path1.startsWith("/foo")).isTrue();
        assertThat(path1.startsWith("/foo/")).isTrue();
        assertThat(path1.startsWith("/foo/bar")).isTrue();
        assertThat(path1.startsWith("/foo/bar/")).isTrue();
        assertThat(path1.startsWith("/foo/bar/baz")).isTrue();
        assertThat(path1.startsWith("/foo/bar/baz/")).isFalse();
        assertThat(path2.startsWith("/foo/bar/baz/")).isTrue();

        assertThat(path1.startsWith("foo")).isFalse();
        assertThat(path3.startsWith("foo")).isTrue();
        assertThat(path3.startsWith("foo/")).isTrue();
        assertThat(path3.startsWith("foo/bar")).isTrue();

        assertThat(path1.startsWith("/bar")).isFalse();
        assertThat(path1.startsWith("bar")).isFalse();
    }

    @Test
    void endsWith() {
        final var path1 = path("/foo/bar/baz");
        final var path2 = path("/foo/bar/baz/");

        assertThat(path1.endsWith("baz")).isTrue();
        assertThat(path1.endsWith("/baz")).isTrue();
        assertThat(path1.endsWith("bar/baz")).isTrue();
        assertThat(path1.endsWith("/bar/baz")).isTrue();
        assertThat(path1.endsWith("foo/bar/baz")).isTrue();
        assertThat(path1.endsWith("/foo/bar/baz")).isTrue();
        assertThat(path1.endsWith("baz/")).isFalse();
        assertThat(path2.endsWith("baz/")).isTrue();

        assertThat(path1.endsWith("foo")).isFalse();
        assertThat(path1.endsWith("bar")).isFalse();
    }

    @Test
    void toAbsolutePath() {
        final var root = path("/");
        final var path1 = path("/foo/bar/baz");
        final var path2 = path("/foo/bar/baz/");
        final var path3 = path("foo/bar/baz");
        final var path4 = path("foo/bar/baz/");

        assertThat(root.toAbsolutePath().toString()).isEqualTo("/");
        assertThat(path1.toAbsolutePath().toString()).isEqualTo(path1.toString());
        assertThat(path2.toAbsolutePath().toString()).isEqualTo(path2.toString());
        assertThat(path3.toAbsolutePath().toString()).isEqualTo(path1.toString());
        assertThat(path4.toAbsolutePath().toString()).isEqualTo(path2.toString());
    }

    @Test
    void toRealPath() {
        final var root = path("/");
        final var path1 = path("/foo/./baz");
        final var path2 = path("/foo/../baz");
        final var path3 = path("/foo/../baz/");
        final var path4 = path("/foo/bar/baz/.");
        final var path5 = path("/foo/bar/baz/..");
        final var path6 = path("foo/./baz");
        final var path7 = path("./baz");
        final var path8 = path("../baz");

        assertThat(root.toRealPath().toString()).isEqualTo("/");
        assertThat(path1.toRealPath().toString()).isEqualTo("/foo/baz");
        assertThat(path2.toRealPath().toString()).isEqualTo("/baz");
        assertThat(path3.toRealPath().toString()).isEqualTo("/baz/");
        assertThat(path4.toRealPath().toString()).isEqualTo("/foo/bar/baz/");
        assertThat(path5.toRealPath().toString()).isEqualTo("/foo/bar/");
        assertThat(path6.toRealPath().toString()).isEqualTo("/foo/baz");
        assertThat(path7.toRealPath().toString()).isEqualTo("/baz");
        assertThat(path8.toRealPath().toString()).isEqualTo("/baz");
    }

    @Test
    void normalize() {
        final var root = path("/");
        final var path1 = path("/foo/./baz");
        final var path2 = path("/foo/../baz");
        final var path3 = path("/foo/../baz/");
        final var path4 = path("/foo/bar/baz/.");
        final var path5 = path("/foo/bar/baz/..");
        final var path6 = path("foo/./baz");
        final var path7 = path("./baz");
        final var path8 = path("../baz");

        assertThat(root.normalize().toString()).isEqualTo("/");
        assertThat(path1.normalize().toString()).isEqualTo("/foo/baz");
        assertThat(path2.normalize().toString()).isEqualTo("/baz");
        assertThat(path3.normalize().toString()).isEqualTo("/baz/");
        assertThat(path4.normalize().toString()).isEqualTo("/foo/bar/baz/");
        assertThat(path5.normalize().toString()).isEqualTo("/foo/bar/");
        assertThat(path6.normalize().toString()).isEqualTo("foo/baz");
        assertThat(path7.normalize().toString()).isEqualTo("baz");
        assertThat(path8.normalize().toString()).isEqualTo("baz");
    }

    @Test
    void resolve() {
        final var root = path("/");
        var path = root.resolve("foo");
        assertThat(path.toString()).isEqualTo("/foo");

        path = root.resolve("/foo");
        assertThat(path.toString()).isEqualTo("/foo");

        path = path.resolve("/bar");
        assertThat(path.toString()).isEqualTo("/bar");

        final var bar = path;
        path = bar.resolve("foo");
        assertThat(path.toString()).isEqualTo("/bar/foo");

        path = bar.resolve("./baz");
        assertThat(path.toString()).isEqualTo("/bar/./baz");

        path = bar.resolve("../baz");
        assertThat(path.toString()).isEqualTo("/bar/../baz");
    }

    @Test
    void relativize() {
        final var empty = path(EMPTY_PATH);
        final var root = path("/");
        final var foo = path("/foo");
        final var fooR = path("foo");
        final var fooDir = path("foo/");
        final var bar = path("/foo/bar/");
        final var barDir = path("bar/");
        final var fooBar = path("foo/bar/");
        final var bof = path("/foo/bar/baz/bof");
        final var bazBof = path("baz/bof");
        final var bazBofDir = path("baz/bof/");
        final var dof = path("baz/bof/dof/");
        final var dofDir = path("dof/");

        assertThat(root.relativize(root)).isEqualTo(empty);
        assertThat(root.relativize(foo)).isEqualTo(fooR);
        assertThat(foo.relativize(foo)).isEqualTo(empty);

        assertThat(bof.relativize(bar).toString()).isEqualTo("../..");
        assertThat(bar.relativize(bof)).isEqualTo(bazBof);

        assertThat(fooBar.relativize(fooDir).toString()).isEqualTo("..");
        assertThat(fooDir.relativize(fooBar)).isEqualTo(barDir);

        assertThat(bazBofDir.relativize(dof)).isEqualTo(dofDir);

        assertThat(empty.relativize(fooR)).isEqualTo(fooR);
    }

    @Test
    void toURI() {
        final var root = path("/");
        final var path1 = path("/foo/./baz");
        final var path2 = path("/foo/../baz");
        final var path3 = path("/foo/../baz/");
        final var path4 = path("/foo/bar/baz/.");
        final var path5 = path("/foo/bar/baz/..");
        final var path6 = path("foo/./baz");
        final var path7 = path("./baz");
        final var path8 = path("../baz");

        assertThat(root.toUri().toString()).isEqualTo("testing://bucket1/");
        assertThat(path1.toUri().toString()).isEqualTo("testing://bucket1/foo/baz");
        assertThat(path2.toUri().toString()).isEqualTo("testing://bucket1/baz");
        assertThat(path3.toUri().toString()).isEqualTo("testing://bucket1/baz/");
        assertThat(path4.toUri().toString()).isEqualTo("testing://bucket1/foo/bar/baz/");
        assertThat(path5.toUri().toString()).isEqualTo("testing://bucket1/foo/bar/");
        assertThat(path6.toUri().toString()).isEqualTo("testing://bucket1/foo/baz");
        assertThat(path7.toUri().toString()).isEqualTo("testing://bucket1/baz");
        assertThat(path8.toUri().toString()).isEqualTo("testing://bucket1/baz");
    }

    @Test
    void iterator() {
        assertThat(toList(path("/foo/bar/baz").iterator())).containsExactly("/foo/", "bar/", "baz");
        assertThat(toList(path("/foo/bar/baz/").iterator())).containsExactly("/foo/", "bar/", "baz/");
        assertThat(toList(path("foo/bar/baz").iterator())).containsExactly("foo/", "bar/", "baz");
    }

    @Test
    void unsupportedMethods() {
        final var root = path("/");
        assertThatThrownBy(root::toFile);
        assertThatThrownBy(() -> root.register(null));
        assertThatThrownBy(() -> root.register(null, null, (Modifier[]) null));
    }

    private StoragePath path(String path) {
        return path(PathRepresentation.of(path));
    }

    private StoragePath path(PathRepresentation path) {
        return path(system, path);
    }

    private static <E> List<String> toList(Iterator<E> iterator) {
        final var list = Lists.mutable.<String>empty();
        while (iterator.hasNext()) {
            list.add(iterator.next().toString());
        }
        return list;
    }

    private static StoragePath path(StorageSystem system, PathRepresentation path) {
        return new StoragePath(system, path);
    }
}
