package org.neo4j.graphdb.traversal;

class MultiPruneEvaluator implements PruneEvaluator
{
    private final PruneEvaluator[] prunings;
    
    MultiPruneEvaluator( PruneEvaluator... prunings )
    {
        this.prunings = prunings;
    }

    public boolean pruneAfter( Position position )
    {
        for ( PruneEvaluator pruner : this.prunings )
        {
            if ( pruner.pruneAfter( position ) )
            {
                return true;
            }
        }
        return false;
    }
    
    public MultiPruneEvaluator add( PruneEvaluator pruner )
    {
        PruneEvaluator[] newPrunings = new PruneEvaluator[this.prunings.length+1];
        System.arraycopy( this.prunings, 0, newPrunings, 0, this.prunings.length );
        newPrunings[newPrunings.length-1] = pruner;
        return new MultiPruneEvaluator( newPrunings );
    }
}
