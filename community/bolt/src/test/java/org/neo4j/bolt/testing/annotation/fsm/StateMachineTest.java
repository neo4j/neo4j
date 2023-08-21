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
package org.neo4j.bolt.testing.annotation.fsm;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.TestTemplate;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.testing.annotation.Version;

@Documented
@TestTemplate
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface StateMachineTest {

    Version[] include() default {};

    Version[] exclude() default {};

    Version until() default @Version(major = ProtocolVersion.MAX_MAJOR_BIT, minor = ProtocolVersion.MAX_MINOR_BIT);

    Version since() default @Version(major = 0, minor = 0);
}
