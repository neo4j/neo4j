package org.neo4j.router.transaction;

import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.kernel.database.DatabaseReference;

/**
 * Container for all information used when deciding routing location
 */
public record RoutingInfo(
        DatabaseReference sessionDatabaseReference, RoutingContext routingContext, AccessMode accessMode) {}
