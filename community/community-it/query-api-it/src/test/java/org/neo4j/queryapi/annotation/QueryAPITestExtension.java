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
package org.neo4j.queryapi.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.ClassTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.queryapi.annotation.support.QueryAPITestSupportExtension;
import org.neo4j.queryapi.testclient.QueryContentType;

/**
 * Extension for running test against Query API.
 * <p/>
 * This extension initiates a {@link org.neo4j.dbms.api.DatabaseManagementService} for
 * each {@link BoltTransportType} configured before all tests start to run.
 * {@link org.neo4j.server.queryapi.tx.TransactionManager} and
 * {@link org.neo4j.queryapi.testclient.QueryAPITestClient} are also created.
 * Those are types are resolved for parameters in methods and constructor of classes
 * annotated with this annotation.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@ClassTemplate
@ExtendWith(QueryAPITestSupportExtension.class)
public @interface QueryAPITestExtension {

    /**
     * The list of types used for transport Bolt messages
     */
    BoltTransportType[] boltTransports() default {
        BoltTransportType.LOCAL_CHANNEL_PACKSTREAM, BoltTransportType.LOCAL_CHANNEL_POJO
    };

    /**
     * When true, enables the authentication in the dbms.
     * The {@link org.neo4j.queryapi.testclient.QueryAPITestClient} is configure with
     * the auth params.
     * The credentials must be changed.
     */
    boolean authEnabled() default false;

    /**
     * When true, enables a sleep procedure on the dbms for testing proposes
     */
    boolean sleepProcedureEnabled() default false;

    /**
     * QueryAPI transaction timeout in seconds.
     * <p/>
     * Negative numbers are ignored, the database default is used instead.
     */
    int queryApiTransactionTimeoutInSeconds() default -1;

    /**
     * Transaction timeout in seconds.
     * <p/>
     * Negative numbers are ignored, the database default is used instead.
     */
    int transactionTimeoutInSeconds() default -1;

    /**
     * Bookmark ready timeout in seconds.
     * <p/>
     * Negative numbers are ignored, the database default is used instead.
     */
    int bookmarkReadyTimeoutInSeconds() default -1;

    /**
     * Enable feature UUID
     * @deprecated Will be removed when UUID is released
     */
    boolean enabledFeatureFlagForUUID() default false;

    /**
     * The content type which the body of the request are encoded on the provided
     * {@link org.neo4j.queryapi.testclient.QueryAPITestClient}
     */
    QueryContentType contentType() default QueryContentType.UNTYPED;

    /**
     * The list of accepted content types which the body of the response received by the provided
     * {@link org.neo4j.queryapi.testclient.QueryAPITestClient}
     */
    QueryContentType[] acceptedContentTypes() default {QueryContentType.UNTYPED};
}
