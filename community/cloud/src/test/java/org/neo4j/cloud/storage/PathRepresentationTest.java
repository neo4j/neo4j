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
import static org.neo4j.cloud.storage.PathRepresentation.EMPTY_PATH;
import static org.neo4j.cloud.storage.PathRepresentation.ROOT;
import static org.neo4j.cloud.storage.PathRepresentation.of;

import org.junit.jupiter.api.Test;

class PathRepresentationTest {

    @Test
    void isRoot() {
        assertThat(ROOT.isRoot()).isTrue();
        assertThat(EMPTY_PATH.isRoot()).isFalse();
        assertThat(of("/foo").isRoot()).isFalse();
        assertThat(of("foo").isRoot()).isFalse();
    }

    @Test
    void isAbsolute() {
        assertThat(ROOT.isAbsolute()).isTrue();
        assertThat(of("/foo").isAbsolute()).isTrue();
        assertThat(of("/foo/bar").isAbsolute()).isTrue();

        assertThat(EMPTY_PATH.isAbsolute()).isFalse();
        assertThat(of("foo").isAbsolute()).isFalse();
        assertThat(of("foo/bar").isAbsolute()).isFalse();
    }

    @Test
    void isDirectory() {
        assertThat(EMPTY_PATH.isDirectory()).isTrue();
        assertThat(ROOT.isDirectory()).isTrue();
        assertThat(of("/foo/").isDirectory()).isTrue();
        assertThat(of("/foo/bar/").isDirectory()).isTrue();
        assertThat(of("foo/").isDirectory()).isTrue();
        assertThat(of("foo/bar/").isDirectory()).isTrue();
        assertThat(of("foo/bar/.").isDirectory()).isTrue();
        assertThat(of("foo/bar/..").isDirectory()).isTrue();

        assertThat(of("/foo").isDirectory()).isFalse();
        assertThat(of("/foo/bar").isDirectory()).isFalse();
        assertThat(of("foo").isDirectory()).isFalse();
        assertThat(of("foo/bar").isDirectory()).isFalse();
    }

    @Test
    void hasTrailingSeparator() {
        assertThat(ROOT.hasTrailingSeparator()).isTrue();
        assertThat(of("/foo/").hasTrailingSeparator()).isTrue();
        assertThat(of("/foo/bar/").hasTrailingSeparator()).isTrue();
        assertThat(of("foo/").hasTrailingSeparator()).isTrue();
        assertThat(of("foo/bar/").hasTrailingSeparator()).isTrue();

        assertThat(EMPTY_PATH.hasTrailingSeparator()).isFalse();
        assertThat(of("/foo").hasTrailingSeparator()).isFalse();
        assertThat(of("/foo/bar").hasTrailingSeparator()).isFalse();
        assertThat(of("foo").hasTrailingSeparator()).isFalse();
        assertThat(of("foo/bar").hasTrailingSeparator()).isFalse();
        assertThat(of("foo/bar/.").hasTrailingSeparator()).isFalse();
        assertThat(of("foo/bar/..").hasTrailingSeparator()).isFalse();
    }

    @Test
    void elements() {
        assertThat(ROOT.elements()).containsExactly();
        assertThat(EMPTY_PATH.elements()).containsExactly();
        assertThat(of("/foo").elements()).containsExactly("foo");
        assertThat(of("/foo/").elements()).containsExactly("foo");
        assertThat(of("foo").elements()).containsExactly("foo");
        assertThat(of("foo/").elements()).containsExactly("foo");
        assertThat(of("/foo/bar").elements()).containsExactly("foo", "bar");
        assertThat(of("/foo/bar/").elements()).containsExactly("foo", "bar");
        assertThat(of("foo/bar").elements()).containsExactly("foo", "bar");
        assertThat(of("foo/bar/").elements()).containsExactly("foo", "bar");
        assertThat(of("/foo/bar/baz").elements()).containsExactly("foo", "bar", "baz");
        assertThat(of("/foo/bar/baz/").elements()).containsExactly("foo", "bar", "baz");
        assertThat(of("foo/bar/baz").elements()).containsExactly("foo", "bar", "baz");
        assertThat(of("foo/bar/baz/").elements()).containsExactly("foo", "bar", "baz");
        assertThat(of(".").elements()).containsExactly(".");
        assertThat(of("..").elements()).containsExactly("..");
        assertThat(of("/foo/bar/.").elements()).containsExactly("foo", "bar", ".");
        assertThat(of("/foo/bar/..").elements()).containsExactly("foo", "bar", "..");
    }

    @Test
    void getParent() {
        final var foo = of("/foo/");
        final var bar = of("/foo/bar/");
        final var baz = of("/foo/bar/baz");
        final var bof = of("/foo/bar/bof/");

        final var fooR = of("foo/");
        final var barR = of("foo/bar/");
        final var bazR = of("foo/bar/baz");
        final var bofR = of("foo/bar/bof/");

        assertThat(ROOT.getParent()).isNull();
        assertThat(baz.getParent()).isEqualTo(bar);
        assertThat(bof.getParent()).isEqualTo(bar);
        assertThat(bar.getParent()).isEqualTo(foo);
        assertThat(foo.getParent()).isEqualTo(ROOT);

        assertThat(bazR.getParent()).isEqualTo(barR);
        assertThat(bofR.getParent()).isEqualTo(barR);
        assertThat(barR.getParent()).isEqualTo(fooR);
        assertThat(fooR.getParent()).isNull();
    }

    @Test
    void subPath() {
        final var path1 = of("/foo/bar/baz");
        final var path2 = of("/foo/bar/baz/");

        assertThat(path1.subpath(0, 1)).isEqualTo(of("foo/"));
        assertThat(path1.subpath(0, 2)).isEqualTo(of("foo/bar/"));
        assertThat(path1.subpath(0, 3)).isEqualTo(of("foo/bar/baz"));
        assertThat(path1.subpath(1, 2)).isEqualTo(of("bar/"));
        assertThat(path1.subpath(1, 3)).isEqualTo(of("bar/baz"));
        assertThat(path1.subpath(2, 3)).isEqualTo(of("baz"));

        assertThat(path2.subpath(0, 1)).isEqualTo(of("foo/"));
        assertThat(path2.subpath(0, 2)).isEqualTo(of("foo/bar/"));
        assertThat(path2.subpath(0, 3)).isEqualTo(of("foo/bar/baz/"));
        assertThat(path2.subpath(1, 2)).isEqualTo(of("bar/"));
        assertThat(path2.subpath(1, 3)).isEqualTo(of("bar/baz/"));
        assertThat(path2.subpath(2, 3)).isEqualTo(of("baz/"));
    }
}
