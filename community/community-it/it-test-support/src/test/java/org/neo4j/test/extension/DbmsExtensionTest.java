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

import static org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor.ENGINE_ID;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.testkit.engine.EventConditions.event;
import static org.junit.platform.testkit.engine.EventConditions.finishedWithFailure;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.instanceOf;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.message;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContextException;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.Events;
import org.neo4j.test.extension.dbms.DbmsExtensionCheckCallbackSignature;
import org.neo4j.test.extension.dbms.DbmsExtensionEnforceAnnotations;
import org.neo4j.test.extension.dbms.DbmsExtensionMixImpermanent;

class DbmsExtensionTest {
    @Test
    void enforceAnnotation() {
        Events testEvents = EngineTestKit.engine(ENGINE_ID)
                .selectors(selectClass(DbmsExtensionEnforceAnnotations.class))
                .enableImplicitConfigurationParameters(true)
                .execute()
                .testEvents();

        testEvents
                .assertThatEvents()
                .haveExactly(
                        1,
                        event(finishedWithFailure(
                                instanceOf(IllegalArgumentException.class),
                                message(message -> message.contains("must be annotated")))));
    }

    @Test
    void checkCallbackSignature() {
        Events testEvents = EngineTestKit.engine(ENGINE_ID)
                .selectors(selectClass(DbmsExtensionCheckCallbackSignature.class))
                .enableImplicitConfigurationParameters(true)
                .execute()
                .testEvents();

        testEvents
                .assertThatEvents()
                .haveExactly(
                        1,
                        event(finishedWithFailure(
                                instanceOf(IllegalArgumentException.class),
                                message(message -> message.contains("must return void")))));
        testEvents
                .assertThatEvents()
                .haveExactly(
                        1,
                        event(finishedWithFailure(
                                instanceOf(IllegalArgumentException.class),
                                message(message ->
                                        message.contains("must take one parameter that is assignable from")))));
        testEvents
                .assertThatEvents()
                .haveExactly(
                        1,
                        event(finishedWithFailure(
                                instanceOf(IllegalArgumentException.class),
                                message(message -> message.contains("cannot be found.")))));
    }

    @Test
    void mixImpermanent() {
        Events testEvents = EngineTestKit.engine(ENGINE_ID)
                .selectors(selectClass(DbmsExtensionMixImpermanent.class))
                .enableImplicitConfigurationParameters(true)
                .execute()
                .testEvents();

        testEvents
                .assertThatEvents()
                .haveExactly(1, event(finishedWithFailure(instanceOf(ExtensionContextException.class))));
    }
}
