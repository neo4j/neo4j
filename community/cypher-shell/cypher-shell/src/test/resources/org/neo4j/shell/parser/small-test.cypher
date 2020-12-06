// This file when parsed should return 11 statements

// This is a single line with a single statement...
MATCH (n) RETURN n LIMIT 10

// Similar single line with a single statement...
MATCH (n) RETURN n LIMIT 10;

// This is a single line with three statements...
RETURN "testing"; RETURN "testing"; RETURN "one, two, three"

// This is a single statement across multiple lines...

MATCH (n)
WHERE n.volume > 10
RETURN n AS loud

// And this is a mix of variable length statements, separated by ;...

CREATE (n:Amplifier {volume:11})
;
MATCH (amps) WHERE n.volume > 10
RETURN amps;
RETURN "Encore!"; RETURN "Encore!!"; RETURN "Encore!!!"
