# -*- mode: Python; coding: utf-8 -*-

# Copyright (c) 2002-2011 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Neo4j Python utilities.
"""


from sys import exc_info
import traceback

try:
    from functools import update_wrapper
except:
    # No-op update_wrapper
    def update_wrapper(wrapper, method):
        return wrapper

class PythonicIterator(object):
    def __init__(self, iterator):
        self.__iter = iterator
        
    def __iter__(self):
        return self
        
    def next(self):
        return self.__iter.next()
        
    def close(self):
        self.__iter.close()
      
    def single(self):
        for item in self:
            break
        else: # empty iterator
            return None
        for item in self:
            raise ValueError("Too many items in the iterator")
        try:
            self.close()
        except:
            pass
        return item
    single = property(single)
   
    def __len__(self):
       count = 0
       for it in self:
           count += 1
       return count
        
def rethrow_current_exception_as(ErrorClass):
    # Because exceptions that come out of
    # jython don't subclass exception, but
    # the ones from JPype do, and because
    # they behave slightly differently,
    # we use this boilerplate,.
    t, e, trace = exc_info()
    
    if isinstance(e,tuple) and len(e) > 1 and isinstance(e[1],Exception):
        e = e[1]
    
    if hasattr(e, "message"):
        msg = e.message() if hasattr(e.message, '__call__') else e.message
    else:
        msg = str(e)
    raise ErrorClass(msg), None, trace
        
