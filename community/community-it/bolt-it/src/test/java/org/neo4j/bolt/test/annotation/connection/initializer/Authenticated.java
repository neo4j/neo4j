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
package org.neo4j.bolt.test.annotation.connection.initializer;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.InitializeConnection;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.test.connection.initializer.AuthenticateConnectionInitializer;
import org.neo4j.bolt.test.provider.ConnectionProvider;
import org.neo4j.bolt.testing.client.TransportConnection;

/**
 * Ensures that the annotated {@link TransportConnection} parameter in a {@link BoltTestExtension} is negotiated and
 * authenticated prior to invoking the test.
 * <p />
 * This annotation is applicable to {@link TransportConnection} and {@link ConnectionProvider} parameters within test
 * functions annotated using the {@link ProtocolTest} and/or {@link TransportTest} annotations.
 * <p />
 * This annotation is a meta-annotation and implies {@link Negotiated}.
 */
@Documented
@Negotiated
@Retention(RetentionPolicy.RUNTIME)
@InitializeConnection(AuthenticateConnectionInitializer.class)
@Target({ElementType.ANNOTATION_TYPE, ElementType.PARAMETER})
public @interface Authenticated {

    /**
     * Defines the principal which shall be authenticated.
     *
     * @return a principal.
     */
    String principal() default "";

    /**
     * Defines the credentials which shall be used to authenticate the selected {@link #principal()}.
     * <p />
     * This property is ignored unless {@link #principal()} is set to a non-empty string.
     *
     * @return a password.
     */
    String credentials() default "";
}
