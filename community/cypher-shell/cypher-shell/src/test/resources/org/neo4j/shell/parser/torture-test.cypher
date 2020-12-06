// This file contains 8 statements

CREATE
      (
      // Comment haha!!
      n
      /* Block comment to confuse */ )
  RETURN
      n

 // Some semicolons
   RETURN 1; RETURN 2; /* Evil block comment! */ RETURN 3

    // No semicolons! And newlines!!

  RETURN 1 RETURN
 2 RETURN 3

// This is one statement
MATCH (null)-[:merge]->(true)
with null.delete as foreach, `true`.false as null
return 2 + foreach, coalesce(null, 3.1415)
limit 10;
