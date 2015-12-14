package org.neo4j.kernel.impl.api;

import java.io.IOException;

import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.transaction.command.Command;

/**
 * Wraps several {@link TransactionApplier}s. In this case, each individual visit-call will delegate to {@link
 * #visit(Command)} instead, which will call each wrapped {@link TransactionApplier} in turn. In {@link #close()},
 * the appliers are closed in reversed order.
 */
public class TransactionApplierFacade extends TransactionApplier.Adapter
{

    final TransactionApplier[] appliers;

    public TransactionApplierFacade( TransactionApplier... appliers )
    {
        this.appliers = appliers;
    }

    @Override
    public void close() throws Exception
    {
        // Not sure why it is necessary to close them in reverse order
        for ( int i = appliers.length; i-- > 0; )
        {
            appliers[i].close();
        }
    }

    @Override
    public boolean visit( Command element ) throws IOException
    {
        boolean result = false;
        for ( TransactionApplier applier : appliers )
        {
            result |= element.handle( applier );
        }
        return result;
    }

    @Override
    public boolean visitNodeCommand( Command.NodeCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitRelationshipCommand( Command.RelationshipCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitPropertyCommand( Command.PropertyCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitRelationshipGroupCommand( Command.RelationshipGroupCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitRelationshipTypeTokenCommand( Command.RelationshipTypeTokenCommand command )
            throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitLabelTokenCommand( Command.LabelTokenCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitPropertyKeyTokenCommand( Command.PropertyKeyTokenCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitSchemaRuleCommand( Command.SchemaRuleCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitNeoStoreCommand( Command.NeoStoreCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitIndexAddNodeCommand( IndexCommand.AddNodeCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitIndexAddRelationshipCommand( IndexCommand.AddRelationshipCommand command )
            throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitIndexRemoveCommand( IndexCommand.RemoveCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitIndexDeleteCommand( IndexCommand.DeleteCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitIndexCreateCommand( IndexCommand.CreateCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitIndexDefineCommand( IndexDefineCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitNodeCountsCommand( Command.NodeCountsCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitRelationshipCountsCommand( Command.RelationshipCountsCommand command ) throws IOException
    {
        return visit( command );
    }
}
