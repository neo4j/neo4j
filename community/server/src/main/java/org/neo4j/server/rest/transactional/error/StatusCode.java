package org.neo4j.server.rest.transactional.error;

/*
 * Put in place as an enum to enforce all error codes remaining collected in one location.
 * Note: These codes will be exposed to the user through our API, although for now they will
 * remain undocumented. There is a discussion to be had about these codes and how we should
 * categorize and pick them.
 *
 * The categories below are an initial proposal, we should have a real discussion about this before
 * anything is documented.
 */
public enum StatusCode
{
    // informal naming convention:
    // *_ERROR:    Transaction is rolled back / aborted
    // INVALID_*:  No change to transaction state (i.e. request is just rejected)

    // 3xxxxx Communication protocol errors
    COMMUNICATION_ERROR( 30000 ),

    // 4xxxxx User errors
    INVALID_REQUEST( 40000 ),
    INVALID_REQUEST_FORMAT( 40001 ),

    INVALID_TRANSACTION_ID( 40010 ),
    INVALID_CONCURRENT_TRANSACTION_ACCESS( 40011 ),

    STATEMENT_EXECUTION_ERROR( 42000 ),
    STATEMENT_SYNTAX_ERROR( 42001 ),
    STATEMENT_MISSING_PARAMETER_ERROR( 42002 ),

    // 5xxxxx Database errors
    INTERNAL_DATABASE_ERROR( 50000 ),
    INTERNAL_STATEMENT_EXECUTION_ERROR( 50001 ),

    INTERNAL_BEGIN_TRANSACTION_ERROR( 53010 ),
    INTERNAL_ROLLBACK_TRANSACTION_ERROR( 53011 ),
    INTERNAL_COMMIT_TRANSACTION_ERROR( 53012 );

    private final int code;

    StatusCode( int code )
    {
        this.code = code;
    }

    public int getCode()
    {
        return code;
    }
}
