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
package org.neo4j.commandline.fleetmanagement;

import static org.neo4j.commandline.fleetmanagement.util.Utilities.getOutputStream;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.neo4j.bolt.discovery.packet.DiscoveryConstants;
import org.neo4j.cli.AbstractCommand;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.fleetmanagement.model.Dbmss;
import org.neo4j.commandline.fleetmanagement.model.Server;
import org.neo4j.commandline.fleetmanagement.network.NetworkListener;
import org.neo4j.commandline.fleetmanagement.output.Csv;
import org.neo4j.commandline.fleetmanagement.output.Format;
import org.neo4j.commandline.fleetmanagement.output.IFormat;
import org.neo4j.commandline.fleetmanagement.output.Json;
import org.neo4j.commandline.fleetmanagement.output.Pretty;
import org.neo4j.commandline.fleetmanagement.util.Utilities;
import org.neo4j.commandline.fleetmanagement.view.DiscoveryView;
import picocli.CommandLine;

@CommandLine.Command(
        name = "discover",
        header = "Discover and list Neo4j servers.",
        description =
                "Listen for Neo4j fleet discovery broadcasts on the local network and list all discovered Neo4j servers.")
public class DiscoverCommand extends AbstractCommand {
    @CommandLine.Option(names = "--timeout", description = "Timeout in seconds.", defaultValue = "60")
    int timeout;

    @CommandLine.Option(names = "--format", description = "Output format (pretty, csv, json).", defaultValue = "Pretty")
    Format format;

    @CommandLine.Option(
            names = "--filename",
            description = "Optional filename to which discovered servers will be written.")
    Path filename;

    public DiscoverCommand(ExecutionContext ctx) {
        super(ctx);
    }

    @Override
    public void execute() throws InterruptedException {
        Map<String, Server> discoveredNodes = new ConcurrentHashMap<>();

        var running = Utilities.boolWithTimeout(timeout, () -> outputResults(discoveredNodes));
        if (format == Format.Pretty || filename != null) {
            System.out.println("Listening for Neo4j discovery broadcasts on port " + DiscoveryConstants.BROADCAST_PORT
                    + " for a duration of " + timeout + " seconds");
            DiscoveryView.startDiscoveryIndicator(running, discoveredNodes);
        }
        new NetworkListener(DiscoveryConstants.BROADCAST_PORT).listen(running, discoveredNodes);
    }

    private void outputResults(Map<String, Server> discoveredNodes) {
        System.out.println();
        var dbmss = Dbmss.fromServers(discoveredNodes.values());

        try (var out = getOutputStream(filename)) {
            IFormat outputFormat =
                    switch (format) {
                        case Pretty -> new Pretty();
                        case Json -> new Json();
                        case Csv -> new Csv();
                    };

            outputFormat.printDiscoveredDbmss(dbmss, out);

            if (filename != null) {
                var serverCount = dbmss.serverCount();
                System.out.println(serverCount + " discovered Neo4j server" + (serverCount != 1 ? "s" : "")
                        + " written to <" + filename + ">");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write output", e);
        }
    }
}
