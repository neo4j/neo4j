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
package org.neo4j.test.extension;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ForkingTestExtension.class)
class ForkingExtensionCases {

    private static boolean beforeAllCalled;
    private boolean beforeEachCalled;

    @BeforeAll
    static void beforeAll() {
        beforeAllCalled = true;
    }

    @BeforeEach
    void beforeEach() {
        beforeEachCalled = true;
    }

    @AfterEach
    void afterEach() {
        assertTrue(beforeEachCalled);
    }

    @AfterAll
    static void afterAll() {
        assertTrue(beforeAllCalled);
    }

    @Test
    void executeInForm() {
        assertTrue(true);
    }

    @Test
    void propagateErrors() {
        fail("Test failure that should propagate from fork to main thread");
    }
}
