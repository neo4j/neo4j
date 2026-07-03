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
package org.neo4j.server.modules;

import java.util.List;
import org.neo4j.configuration.Config;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.queryapi.QueryResource;
import org.neo4j.server.queryapi.metrics.QueryAPIMetricsFilter;
import org.neo4j.server.queryapi.metrics.QueryAPIMetricsMonitor;
import org.neo4j.server.queryapi.request.JsonMessageBodyReader;
import org.neo4j.server.queryapi.request.typed.TypedJsonMessageBodyReaderV1x0;
import org.neo4j.server.queryapi.request.typed.TypedJsonMessageBodyReaderV1x1;
import org.neo4j.server.queryapi.request.typed.TypedJsonMessageBodyReaderV1x2;
import org.neo4j.server.queryapi.response.error.ErrorResponseWriter;
import org.neo4j.server.queryapi.response.error.InternalServerExceptionMapper;
import org.neo4j.server.queryapi.response.error.JsonlErrorResponseWriter;
import org.neo4j.server.queryapi.response.error.Neo4jExceptionMapper;
import org.neo4j.server.queryapi.response.error.QueryApiExceptionMapper;
import org.neo4j.server.queryapi.response.error.WebApplicationExceptionMapper;
import org.neo4j.server.queryapi.response.writer.PlainJsonBookmarkWriter;
import org.neo4j.server.queryapi.response.writer.PlainJsonDriverAutoCommitResultWriter;
import org.neo4j.server.queryapi.response.writer.PlainJsonTxInfoWriter;
import org.neo4j.server.queryapi.response.writer.PlainJsonTxManagingResultWriter;
import org.neo4j.server.queryapi.response.writer.PlainJsonlBookmarkWriter;
import org.neo4j.server.queryapi.response.writer.PlainJsonlDriverAutoCommitResultWriter;
import org.neo4j.server.queryapi.response.writer.PlainJsonlTxInfoWriter;
import org.neo4j.server.queryapi.response.writer.PlainJsonlTxManagingResultWriter;
import org.neo4j.server.queryapi.response.writer.TypedJsonBookmarkWriter;
import org.neo4j.server.queryapi.response.writer.TypedJsonDriverAutoCommitResultWriter;
import org.neo4j.server.queryapi.response.writer.TypedJsonDriverAutoCommitResultWriterV11;
import org.neo4j.server.queryapi.response.writer.TypedJsonDriverAutoCommitResultWriterV12;
import org.neo4j.server.queryapi.response.writer.TypedJsonTxInfoWriter;
import org.neo4j.server.queryapi.response.writer.TypedJsonTxManagingResultWriter;
import org.neo4j.server.queryapi.response.writer.TypedJsonTxManagingResultWriterV11;
import org.neo4j.server.queryapi.response.writer.TypedJsonTxManagingResultWriterV12;
import org.neo4j.server.queryapi.response.writer.TypedJsonlBookmarkWriter;
import org.neo4j.server.queryapi.response.writer.TypedJsonlBookmarkWriterV11;
import org.neo4j.server.queryapi.response.writer.TypedJsonlBookmarkWriterV12;
import org.neo4j.server.queryapi.response.writer.TypedJsonlDriverAutoCommitResultWriter;
import org.neo4j.server.queryapi.response.writer.TypedJsonlDriverAutoCommitResultWriterV11;
import org.neo4j.server.queryapi.response.writer.TypedJsonlDriverAutoCommitResultWriterV12;
import org.neo4j.server.queryapi.response.writer.TypedJsonlTxInfoWriter;
import org.neo4j.server.queryapi.response.writer.TypedJsonlTxInfoWriterV11;
import org.neo4j.server.queryapi.response.writer.TypedJsonlTxInfoWriterV12;
import org.neo4j.server.queryapi.response.writer.TypedJsonlTxManagingResultWriter;
import org.neo4j.server.queryapi.response.writer.TypedJsonlTxManagingResultWriterV11;
import org.neo4j.server.queryapi.response.writer.TypedJsonlTxManagingResultWriterV12;
import org.neo4j.server.web.WebServer;

/**
 * Mounts the Query API
 */
public class QueryModule implements ServerModule {

    private final WebServer webServer;
    private final Config config;

    private final QueryAPIMetricsMonitor metricsMonitor;

    public QueryModule(WebServer webServer, Config config, QueryAPIMetricsMonitor metricsMonitor) {
        this.webServer = webServer;
        this.config = config;
        this.metricsMonitor = metricsMonitor;
    }

    @Override
    public void start() {
        webServer.addJAXRSClasses(
                jaxRsClasses(), config.get(ServerSettings.db_api_path).toString(), null);

        webServer.addFilter(
                new QueryAPIMetricsFilter(metricsMonitor, config.get(ServerSettings.transaction_id_length)), "/*");
    }

    @Override
    public void stop() {
        webServer.removeJAXRSClasses(
                jaxRsClasses(), config.get(ServerSettings.db_api_path).toString());
    }

    private static List<Class<?>> jaxRsClasses() {
        return List.of(
                QueryResource.class,
                // JSON
                PlainJsonDriverAutoCommitResultWriter.class,
                TypedJsonDriverAutoCommitResultWriter.class,
                TypedJsonDriverAutoCommitResultWriterV11.class,
                TypedJsonDriverAutoCommitResultWriterV12.class,
                PlainJsonTxManagingResultWriter.class,
                TypedJsonTxManagingResultWriter.class,
                TypedJsonTxManagingResultWriterV11.class,
                TypedJsonTxManagingResultWriterV12.class,
                PlainJsonTxInfoWriter.class,
                TypedJsonTxInfoWriter.class,
                TypedJsonBookmarkWriter.class,
                PlainJsonBookmarkWriter.class,
                JsonMessageBodyReader.class,
                TypedJsonMessageBodyReaderV1x0.class,
                TypedJsonMessageBodyReaderV1x1.class,
                TypedJsonMessageBodyReaderV1x2.class,
                Neo4jExceptionMapper.class,
                QueryApiExceptionMapper.class,
                WebApplicationExceptionMapper.class,
                InternalServerExceptionMapper.class,
                ErrorResponseWriter.class,
                // JSONL
                PlainJsonlDriverAutoCommitResultWriter.class,
                PlainJsonlTxManagingResultWriter.class,
                PlainJsonlBookmarkWriter.class,
                PlainJsonlTxInfoWriter.class,
                TypedJsonlDriverAutoCommitResultWriter.class,
                TypedJsonlDriverAutoCommitResultWriterV11.class,
                TypedJsonlDriverAutoCommitResultWriterV12.class,
                TypedJsonlTxManagingResultWriter.class,
                TypedJsonlTxManagingResultWriterV11.class,
                TypedJsonlTxManagingResultWriterV12.class,
                TypedJsonlBookmarkWriter.class,
                TypedJsonlBookmarkWriterV11.class,
                TypedJsonlBookmarkWriterV12.class,
                TypedJsonlTxInfoWriter.class,
                TypedJsonlTxInfoWriterV11.class,
                TypedJsonlTxInfoWriterV12.class,
                JsonlErrorResponseWriter.class);
    }
}
