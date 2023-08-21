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
        ConsoleUrlMatcher[] matchers = devMode
                ? new ConsoleUrlMatcher[] {
                    new ConsoleUrlMatcher.DevMatcher(),
                    new ConsoleUrlMatcher.ProdMatcher(),
                    new ConsoleUrlMatcher.PrivMatcher()
                }
                : new ConsoleUrlMatcher[] {new ConsoleUrlMatcher.ProdMatcher(), new ConsoleUrlMatcher.PrivMatcher()};

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

                return new AuraConsole(
                        String.format("https://console%s.neo4j.io", environment == null ? "" : environment),
                        databaseId);
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
                if (matcher.groupCount() == 5) {
                    domain = matcher.group(4);
                }

                String baseURL = String.format("https://console%s.neo4j%s.io", environment, domain);
                return new AuraConsole(baseURL, databaseId);
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
                String baseURL = String.format("https://console%s.neo4j%s.io", environment, domain);
                return new AuraConsole(baseURL, databaseId);
            }
        }
    }
}
