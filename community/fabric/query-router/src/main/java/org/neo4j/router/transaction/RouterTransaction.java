package org.neo4j.router.transaction;

import java.util.Optional;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.transaction.parent.CompoundTransaction;
import org.neo4j.kernel.api.exceptions.Status;

public interface RouterTransaction extends CompoundTransaction<DatabaseTransaction> {
    Optional<Status> getReasonIfTerminated();

    DatabaseTransaction transactionFor(Location location);
}
