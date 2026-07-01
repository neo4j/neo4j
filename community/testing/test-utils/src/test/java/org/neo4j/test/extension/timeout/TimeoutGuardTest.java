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
package org.neo4j.test.extension.timeout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.engine.descriptor.JupiterEngineDescriptor.ENGINE_ID;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.neo4j.internal.helpers.ProcessUtils;

public class TimeoutGuardTest {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    @Test
    void terminateHangingTest() throws IOException {
        assertThat(ProcessUtils.executeJava(out, err, Duration.ofMinutes(5), UncooperativeHangerMain.class.getName()))
                .isEqualTo(1);
        assertThat(out.toString())
                .contains("***WARNING***")
                .contains(
                        "Test monitor terminating hanging execution for test org.neo4j.test.extension.timeout.UncooperativeHanger.hangWithMe")
                .contains(
                        "After the test timeout was reached, an interruption attempt was made; however, the test did not progress within the allocated grace period. Terminating the VM.");
        assertThat(err.toString())
                .contains("***WARNING***")
                .contains(
                        "Test monitor terminating hanging execution for test org.neo4j.test.extension.timeout.UncooperativeHanger.hangWithMe")
                .contains(
                        "After the test timeout was reached, an interruption attempt was made; however, the test did not progress within the allocated grace period. Terminating the VM.");
    }

    private static final class UncooperativeHangerMain {

        public static void main(String[] args) {
            EngineTestKit.engine(ENGINE_ID)
                    .selectors(selectClass(UncooperativeHanger.class))
                    .enableImplicitConfigurationParameters(true)
                    .execute();
        }
    }
}
