mvncommand="mvn"

# prefer the mvn3 command if it exists
hash mvn3 &> /dev/null
if [ $? -eq 0 ]; then
  mvncommand="mvn3"
fi

$mvncommand clean compile -Dcheck "$@"

