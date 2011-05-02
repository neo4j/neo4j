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

def iterator(method):
    global iterator

    class Iterator(object):
        def __init__(self, iterator):
            self.__iter = iterator
        def __iter__(self):
            return iter(self.__iter)
        def single(self):
            iterator = iter(self.__iter)
            for item in iterator:
                break
            else: # empty iterator
                return None
            for item in iterator:
                raise ValueError("Too many items in the iterator")
            try:
                iterator.close()
            except:
                pass
            return item
        single = property(single)

    try:
        from functools import update_wrapper
    except:
        def iterator(method):
            def wrapper(*args,**kwargs):
                return Iterator(method(*args,**kwargs))
            return wrapper
    else:
        def iterator(method):
            def wrapper(*args,**kwargs):
                return Iterator(method(*args,**kwargs))
            return update_wrapper(wrapper, method)

    return iterator(method)
        
