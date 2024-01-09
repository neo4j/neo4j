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

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.DELETE_ON_CLOSE;
import static java.nio.file.StandardOpenOption.DSYNC;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.cloud.storage.StorageUtils.normalizeForRead;
import static org.neo4j.cloud.storage.StorageUtils.normalizeForWrite;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.Set;
import org.junit.jupiter.api.Test;

class StorageUtilsTest {

    @Test
    void invalidOptions() {
        assertThatThrownBy(() -> normalizeForRead(WeirdOption.WHAT_THE)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> normalizeForRead(SYNC)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> normalizeForRead(DSYNC)).isInstanceOf(IOException.class);
    }

    @Test
    void readAndWriteNotSupported() {
        assertThatThrownBy(() -> normalizeForRead(WRITE)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> normalizeForWrite(READ)).isInstanceOf(IOException.class);
        // this create WRITE-like options
        assertThatThrownBy(() -> normalizeForRead(APPEND)).isInstanceOf(IOException.class);
    }

    @Test
    void normalizeJustDefaults() throws IOException {
        assertNormalization(normalizeForRead(), READ);
        assertNormalization(normalizeForWrite(), WRITE);
    }

    @Test
    void removeIgnoredOptions() throws IOException {
        assertNormalization(normalizeForRead(SPARSE, DELETE_ON_CLOSE), READ);
        assertNormalization(normalizeForRead(TRUNCATE_EXISTING), READ);

        assertNormalization(normalizeForWrite(APPEND, TRUNCATE_EXISTING), WRITE);
    }

    @Test
    void retainsSingleCreateOption() throws IOException {
        assertNormalization(normalizeForWrite(CREATE), WRITE, CREATE);
        assertNormalization(normalizeForWrite(CREATE_NEW), WRITE, CREATE_NEW);
        assertNormalization(normalizeForWrite(CREATE, CREATE_NEW), WRITE, CREATE_NEW);
    }

    private void assertNormalization(Set<? extends OpenOption> actual, OpenOption... expected) {
        assertThat(actual).isEqualTo(Set.of(expected));
    }

    private enum WeirdOption implements OpenOption {
        WHAT_THE
    }
}
