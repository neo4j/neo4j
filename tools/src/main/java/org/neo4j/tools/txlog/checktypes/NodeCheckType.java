package org.neo4j.tools.txlog.checktypes;

import java.util.Objects;

import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command;

class NodeCheckType extends CheckType<Command.NodeCommand,NodeRecord>
{
    NodeCheckType()
    {
        super( Command.NodeCommand.class );
    }

    @Override
    public NodeRecord before( Command.NodeCommand command )
    {
        return command.getBefore();
    }

    @Override
    public NodeRecord after( Command.NodeCommand command )
    {
        return command.getAfter();
    }

    @Override
    public boolean equal( NodeRecord record1, NodeRecord record2 )
    {
        Objects.requireNonNull( record1 );
        Objects.requireNonNull( record2 );

        return record1.getId() == record2.getId() &&
               record1.inUse() == record2.inUse() &&
               record1.getNextProp() == record2.getNextProp() &&
               record1.getNextRel() == record2.getNextRel() &&
               record1.isDense() == record2.isDense() &&
               record1.getLabelField() == record2.getLabelField();
    }

    @Override
    public String name()
    {
        return "node";
    }
}
