
from _backend import extends, rel_type,\
    Path, TraverserImpl, Traversal,\
    TraversalDescriptionImpl, strings
    
from core import ANY, DirectionalType

#
# Pythonification of the traversal API
#

class Path(extends(Path)):

    @property
    def start(self): return self.startNode()
    
    @property
    def end(self):   return self.endNode()
    
    @property
    def last_relationship(self): return self.lastRelationship()
    
    def __repr__(self): return self.toString()
    
    def __len__(self):  return self.length()
    
    def __iter__(self):
        for entity in self.iterator():
            yield entity 

      
class TraversalDescriptionImpl(extends(TraversalDescriptionImpl)):
    
    def relationships(self, reltype, direction=ANY):
        if type(reltype) in strings:
            reltype = rel_type(reltype)
        elif isinstance(reltype, DirectionalType):
            direction = reltype.dir
            reltype = reltype.type
        return self._super__relationships(reltype, direction)

           
class TraverserImpl(extends(TraverserImpl)):
    
    def __iter__(self): 
        return self.iterator()
