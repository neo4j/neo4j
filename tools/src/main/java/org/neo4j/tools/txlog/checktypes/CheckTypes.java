package org.neo4j.tools.txlog.checktypes;

import org.neo4j.kernel.impl.store.record.Abstract64BitRecord;
import org.neo4j.kernel.impl.transaction.command.Command;

public class CheckTypes
{
    public static final NodeCheckType NODE = new NodeCheckType();
    public static final PropertyCheckType PROPERTY = new PropertyCheckType();

    @SuppressWarnings( "unchecked" )
    public static final CheckType<? extends Command, ? extends Abstract64BitRecord>[] CHECK_TYPES =
            new CheckType[]{NODE, PROPERTY};
}
