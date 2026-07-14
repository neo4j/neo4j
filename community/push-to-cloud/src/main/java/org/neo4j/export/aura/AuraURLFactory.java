/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.export.aura;

import static java.util.Arrays.stream;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.neo4j.cli.CommandFailedException;

public class AuraURLFactory {

    public AuraConsole buildConsoleURI(String boltURI, boolean devMode) throws CommandFailedException {
        return buildConsoleURI(boltURI, devMode, null);
    }

    public AuraConsole buildConsoleURI(String boltURI, boolean devMode, String dbId) throws CommandFailedException {
        ConsoleUrlMatcher[] matchers = devMode
                ? new ConsoleUrlMatcher[] {
                    new ConsoleUrlMatcher.DevMatcher(),
                    new ConsoleUrlMatcher.ProdMatcher(),
                    new ConsoleUrlMatcher.PrivMatcher(),
                    new ConsoleUrlMatcher.InstanceMatcher(dbId, true)
                }
                : new ConsoleUrlMatcher[] {
                    new ConsoleUrlMatcher.ProdMatcher(),
                    new ConsoleUrlMatcher.PrivMatcher(),
                    new ConsoleUrlMatcher.InstanceMatcher(dbId, false)
                };

        return stream(matchers)
                .filter(m -> m.match(boltURI))
                .findFirst()
                .orElseThrow(() -> new CommandFailedException("Invalid Bolt URI '" + boltURI + "'"))
                .getConsole();
    }

    abstract static class ConsoleUrlMatcher {

        // A boltURI looks something like this:
        //
        //   bolt+routing://mydbid-myenvironment.databases.neo4j.io
        //                  <─┬──><──────┬─────>
        //                    │          └──────── environment
        //                    └─────────────────── database id
        //
        // When running in a dev environment it can also be of the form
        // bolt+routing://mydbid-myenv.databases.neo4j-myenv.io
        // Constructing a console URI takes elements from the bolt URI and places them inside this URI:
        //
        //   https://console<environment>.neo4j.io/v1/databases/<database id>
        //
        // Examples:
        //
        //   bolt+routing://rogue.databases.neo4j.io  --> https://console.neo4j.io/v1/databases/rogue
        //   bolt+routing://rogue-mattias.databases.neo4j.io  --> https://console-mattias.neo4j.io/v1/databases/rogue
        //   bolt+routing://rogue-myenv.databases.neo4j-myenv.io  -->
        // https://console-myenv.neo4j-myenv.io/v1/databases/rogue
        //
        // When PrivateLink is enabled, the URL scheme is a little different:
        //
        //   bolt+routing://mydbid.myenv-orch-0003.neo4j.io"
        //                  <─┬──> <─┬─>
        //                    │      └──────────── environment
        //                    └─────────────────── database id

        protected Matcher matcher;
        protected String url;

        protected abstract Pattern pattern();

        protected static String buildBaseURL(String environment, String domain) {
            return String.format(
                    "https://console%s.neo4j%s.io",
                    environment != null ? environment : "", domain != null ? domain : "");
        }

        public abstract AuraConsole getConsole();

        public boolean match(String url) {
            this.url = url;
            matcher = pattern().matcher(url);
            return matcher.matches();
        }

        static class ProdMatcher extends ConsoleUrlMatcher {
            @Override
            protected Pattern pattern() {
                return Pattern.compile(
                        "(?:bolt(?:\\+routing)?|neo4j(?:\\+s|\\+ssc)?)://([^-]+)(-(.+))?.databases.neo4j.io$");
            }

            @Override
            public AuraConsole getConsole() {
                String databaseId = matcher.group(1);
                String environment = matcher.group(2);
                return new AuraConsole(buildBaseURL(environment, null), databaseId);
            }
        }

        static class DevMatcher extends ConsoleUrlMatcher {
            @Override
            protected Pattern pattern() {
                return Pattern.compile(
                        "(?:bolt(?:\\+routing)?|neo4j(?:\\+s|\\+ssc)?)://([^-]+)(-(.+))?.databases.neo4j(-(.+))?.io$");
            }

            @Override
            public AuraConsole getConsole() {
                String databaseId = matcher.group(1);
                String environment = matcher.group(2);
                String domain = "";

                if (environment == null) {
                    throw new CommandFailedException(
                            "Expected to find an environment running in dev mode in bolt URI: " + url);
                }

                if (matcher.groupCount() == 5 && matcher.group(4) != null) {
                    domain = matcher.group(4);
                }

                return new AuraConsole(buildBaseURL(environment, domain), databaseId);
            }
        }

        static class PrivMatcher extends ConsoleUrlMatcher {
            @Override
            protected Pattern pattern() {
                return Pattern.compile(
                        "(?:bolt(?:\\+routing)?|neo4j(?:\\+s|\\+ssc)?)://([a-zA-Z0-9]+)\\.(\\S+)-orch-(\\d+).neo4j(-\\S+)?.io$");
            }

            @Override
            public AuraConsole getConsole() {
                String databaseId = matcher.group(1);
                String environment = matcher.group(2);
                String domain = "";

                switch (environment) {
                    case "production" -> environment = "";
                    case "staging" -> environment = "-" + environment;
                    default -> {
                        environment = "-" + environment;
                        if (matcher.group(4) == null) {
                            throw new CommandFailedException("Invalid Bolt URI '" + url + "'");
                        }
                        domain = matcher.group(4);
                    }
                }
                return new AuraConsole(buildBaseURL(environment, domain), databaseId);
            }
        }

        static class InstanceMatcher extends ConsoleUrlMatcher {
            private final String dbId;
            private final boolean devMode;

            InstanceMatcher(String dbId, boolean devMode) {
                this.dbId = dbId;
                this.devMode = devMode;
            }

            @Override
            protected Pattern pattern() {
                // Instance-based bolt URIs follow the same convention as ProdMatcher: the
                // environment is an optional suffix on the instance identifier, separated by "-".
                //
                // Examples:
                //   neo4j+s://dbid.instances.neo4j.io
                //   neo4j+s://dbid-staging.instances.neo4j.io
                //   neo4j+s://dbid-rogueenv.instances.neo4j-dev.io
                //
                // Group 1 captures the environment suffix including the leading "-" (e.g. "-staging"),
                // or is null when there is no environment.
                // In dev mode, group 2 captures the optional domain suffix (e.g. "-dev").
                if (devMode) {
                    return Pattern.compile(
                            "(?:bolt(?:\\+routing)?|neo4j(?:\\+s|\\+ssc)?)://[^-]+(-[^.]+)?\\.instances\\.neo4j(-[^.]+)?\\.io$");
                }
                return Pattern.compile(
                        "(?:bolt(?:\\+routing)?|neo4j(?:\\+s|\\+ssc)?)://[^-]+(-[^.]+)?\\.instances\\.neo4j\\.io$");
            }

            @Override
            public AuraConsole getConsole() {
                if (dbId == null || dbId.isBlank()) {
                    throw new CommandFailedException(
                            "--to-dbid must be specified when providing an instance based URI");
                }
                String environment = matcher.group(1);
                String domain = devMode ? matcher.group(2) : null;
                return new AuraConsole(buildBaseURL(environment, domain), dbId);
            }
        }
    }
}
