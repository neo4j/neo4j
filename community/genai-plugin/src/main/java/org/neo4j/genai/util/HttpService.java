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
package org.neo4j.genai.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpResponse.BodySubscriber;
import java.net.http.HttpResponse.ResponseInfo;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.eclipse.collections.api.factory.primitive.IntObjectMaps;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.set.primitive.ImmutableIntSet;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.neo4j.genai.vector.VectorEncoding;

public final class HttpService {

    private static final String USER_AGENT = "Neo4j-GenAIProcedures/" + VectorEncoding.VERSION;

    private final ImmutableIntSet defaultAcceptableStatusCodes = IntSets.immutable.of(200);

    public static final Function<InputStream, Map<String, Object>> DEFAULT_RESPONSE_TO_MAP_TRANSFORMER =
            (inputStream -> {
                try {
                    return JsonUtils.getObjectMapper().readValue(inputStream, JsonUtils.TYPE_REF_MAP_STRING_OBJECT);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

    /**
     * Creates a pipe from an output stream the given consumer should write to, to an {@link InputStream} that is supplied to a {@link BodyPublisher}.
     * The output-stream will be closed in this method, the consumer does not need to take care of it.
     *
     * @param outputStreamConsumer a consumer for the output-stream
     * @return the usable {@link BodyPublisher}
     */
    public static BodyPublisher pipe(Consumer<OutputStream> outputStreamConsumer) {
        return HttpRequest.BodyPublishers.ofInputStream(() -> {
            try (var out = new ByteArrayOutputStream()) {
                outputStreamConsumer.accept(out);
                out.flush();
                return new ByteArrayInputStream(out.toByteArray());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    record Response<R, T>(R value, T error) {}

    static final class BodyAndErrorHandler<R, T> implements BodyHandler<Response<R, T>> {

        private final BodyHandler<R> responseHandler;
        private final BodyHandler<T> errorHandler;

        BodyAndErrorHandler(BodyHandler<R> responseHandler, BodyHandler<T> errorHandler) {
            this.responseHandler = responseHandler;
            this.errorHandler = errorHandler;
        }

        @Override
        public BodySubscriber<Response<R, T>> apply(ResponseInfo responseInfo) {
            if (responseInfo.statusCode() == 200) {
                return HttpResponse.BodySubscribers.mapping(
                        responseHandler.apply(responseInfo), (r) -> new Response<>(r, null));
            } else {
                return HttpResponse.BodySubscribers.mapping(
                        errorHandler.apply(responseInfo), (t) -> new Response<>(null, t));
            }
        }
    }

    /**
     * @see #request(URI, Function, Function, IntSet, IntObjectMap, BiFunction)
     * @param target the target to be requested
     * @param requestCustomizer any customization for a request
     * @param transformer a transformer for the response
     * @return anything that the transformer returns
     * @param <T> the type of the result
     */
    public <T> T request(
            URI target,
            Function<HttpRequest.Builder, HttpRequest> requestCustomizer,
            Function<InputStream, T> transformer) {
        return request(target, requestCustomizer, transformer, defaultAcceptableStatusCodes);
    }

    /**
     * @see #request(URI, Function, Function, IntSet, IntObjectMap, BiFunction)
     * @param target the target to be requested
     * @param requestCustomizer any customization for a request
     * @param transformer a transformer for the response
     * @param acceptableStatusCodes all acceptable status codes
     * @return anything that the transformer returns
     * @param <T> the type of the result
     */
    public <T> T request(
            URI target,
            Function<HttpRequest.Builder, HttpRequest> requestCustomizer,
            Function<InputStream, T> transformer,
            IntSet acceptableStatusCodes) {
        return request(
                target,
                requestCustomizer,
                transformer,
                acceptableStatusCodes,
                IntObjectMaps.immutable.empty(),
                (a, b) -> Optional.empty());
    }

    /**
     * Requests the given {@code target} by initializing a request with some default headers, passing to a customizers
     * and executing it. The customizer can apply body- and argument publishers as they see fit. In case of a successful
     * HTTP 200 response or similar, the response body is given as input-stream to a transformer.
     * @param target the target to be requested
     * @param requestCustomizer any customization for a request
     * @param transformer a transformer for the response
     * @param acceptableStatusCodes all acceptable status codes
     * @param unacceptableStatusCodes unacceptable status codes with corresponding exception suppliers
     * @return anything that the transformer returns
     * @param <T> the type of the result
     */
    public <T> T request(
            URI target,
            Function<HttpRequest.Builder, HttpRequest> requestCustomizer,
            Function<InputStream, T> transformer,
            IntSet acceptableStatusCodes,
            IntObjectMap<Supplier<GenAIProcedureException>> unacceptableStatusCodes,
            BiFunction<Integer, String, Optional<GenAIProcedureException>> providerSpecificStatusHandler) {
        try (var httpClient = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build()) {
            var request =
                    requestCustomizer.apply(HttpRequest.newBuilder().uri(target).header("User-Agent", USER_AGENT));
            var handler = new BodyAndErrorHandler<>(BodyHandlers.ofInputStream(), BodyHandlers.ofString());
            var response = httpClient.send(request, handler);
            var responseCode = response.statusCode();

            if (responseCode == 401) {
                throw new GenAIProcedureException(
                        "Not authorized to make API request; check your credentials.", responseCode);
            }
            if (responseCode == 403) {
                throw new GenAIProcedureException(
                        "API request forbidden (HTTP response code: 403); check your credentials.", responseCode);
            }

            if (!acceptableStatusCodes.contains(responseCode)) {
                var errorMessage = response.body().error();

                throw providerSpecificStatusHandler
                        .apply(responseCode, errorMessage)
                        .orElseGet(unacceptableStatusCodes.getIfAbsent(
                                responseCode,
                                () -> () -> new MalformedGenAIResponseException(
                                        "Unexpected HTTP response code: " + responseCode
                                                + (errorMessage.isBlank() ? "" : " - " + errorMessage),
                                        responseCode)));
            }

            try (var inputStream = response.body().value()) {
                return transformer.apply(inputStream);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GenAIProcedureException("Could not finish request", e);
        }
    }
}
