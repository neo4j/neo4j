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
package org.neo4j.bolt.testing.assertions;

import java.util.function.Consumer;
import org.neo4j.bolt.protocol.common.message.response.ResponseMessage;

@SuppressWarnings("unchecked")
public final class MessageConditions {
    private MessageConditions() {}

    /**
     * Validates both cases and fails only if neither of them succeed (Used because fabric returns a more specific error status in one case)
     */
    public static Consumer<ResponseMessage> either(
            final Consumer<ResponseMessage> caseA, final Consumer<ResponseMessage> caseB) {
        return message -> {
            AssertionError errorA = null;
            AssertionError errorB = null;
            try {
                caseA.accept(message);
            } catch (AssertionError e) {
                errorA = e;
            }
            try {
                caseB.accept(message);
            } catch (AssertionError e) {
                errorB = e;
            }
            if (errorA != null && errorB != null) {
                var err = new AssertionError("Neither case matched");
                err.addSuppressed(errorA);
                err.addSuppressed(errorB);
                throw err;
            }
        };
    }
}
