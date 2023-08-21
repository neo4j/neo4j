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
package org.neo4j.dbms.routing;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.ConfigPatternBuilder;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

public class SimpleClientRoutingDomainChecker implements ClientRoutingDomainChecker {
    protected final InternalLog log;

    private volatile Pattern[] domainPatterns;

    private SimpleClientRoutingDomainChecker(InternalLogProvider logProvider) {
        this.log = logProvider.getLog(this.getClass());
    }

    public static ClientRoutingDomainChecker fromConfig(Config config, InternalLogProvider logProvider) {
        var simpleChecker = new SimpleClientRoutingDomainChecker(logProvider);

        // initialize with the current config
        simpleChecker.setClientRoutingDomain(config.get(GraphDatabaseSettings.client_side_router_enforce_for_domains));

        // listen for changes to the config value in future
        config.addListener(GraphDatabaseSettings.client_side_router_enforce_for_domains, simpleChecker);

        return simpleChecker;
    }

    @Override
    public boolean shouldGetClientRouting(SocketAddress address) {
        // grab a reference to the current array, then all the logic in this method will use the same array.
        Pattern[] patternsToUse = this.domainPatterns;
        return shouldGetClientRouting(address, patternsToUse);
    }

    @Override
    public boolean isEmpty() {
        return domainPatterns == null || domainPatterns.length == 0;
    }

    boolean shouldGetClientRouting(SocketAddress address, Pattern[] patternsToUse) {
        if (patternsToUse.length == 0) {
            return false;
        }

        return isMatch(patternsToUse, address);
    }

    private static boolean isMatch(Pattern[] patterns, SocketAddress address) {
        return Arrays.stream(patterns)
                        .anyMatch(p -> p.matcher(address.getHostname()).matches())
                || Arrays.stream(patterns)
                        .anyMatch(p -> p.matcher(address.toString()).matches());
    }

    /*
     * SettingChangeListener interface. This is called whenever the relevant configuration value is changed by the user.
     * n.b. this method is synchronized to avoid weird races if setting is updated concurrently.
     */
    @Override
    public synchronized void accept(Set<String> before, Set<String> after) {
        if (Objects.equals(before, after)) {
            return;
        }

        setClientRoutingDomain(after);
    }

    private void setClientRoutingDomain(Set<String> after) {
        Pattern[] newDomains = processPatterns(after);
        update(newDomains);
    }

    protected void update(Pattern[] newDomains) {
        this.domainPatterns = newDomains;
    }

    protected Pattern[] processPatterns(Set<String> userProvidedPatterns) {
        return userProvidedPatterns.stream()
                .map(p -> ConfigPatternBuilder.patternFromConfigString(p, Pattern.CASE_INSENSITIVE))
                .toArray(Pattern[]::new);
    }
}
