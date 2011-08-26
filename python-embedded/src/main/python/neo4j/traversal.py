
from _backend import extends, implements, rel_type,\
    Path, TraverserImpl, Traversal,\
    TraversalDescriptionImpl, strings,\
    Evaluation, Evaluator, Uniqueness
        
from core import Direction, DirectionalType

class DynamicEvaluator(implements(Evaluator)):
    def __init__(self, eval_method):
        self._eval_method = eval_method

    def evaluate(self, path):
        return self._eval_method(path)

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
    
    def relationships(self, reltype, direction=Direction.ANY):
        if type(reltype) in strings:
            reltype = rel_type(reltype)
        elif isinstance(reltype, DirectionalType):
            direction = reltype.dir
            reltype = reltype.type
        return self._super__relationships(reltype, direction)
        
    def evaluator(self, ev):
        if hasattr(ev, '__call__'):
            ev = DynamicEvaluator(ev)
        return self._super__evaluator(ev)
           
class TraverserImpl(extends(TraverserImpl)):
    pass
