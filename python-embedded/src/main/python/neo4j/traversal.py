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

"""Neo4j traversal dsl for Python.
"""

class PathTraversal(object):
    def __init__(self, *path):
        self.__path = path
    def __div__(self, other):
        if isinstance(other, PathTraversal):
            other = other.__path
        elif isinstance(other, (list,tuple)):
            other = tuple(other)
        else:
            other = other,
        return PathTraversal(*(self.__path + other))
    __truediv__ = __div__
    def __repr__(self):
        return ' / '.join([repr(item) for item in self.__path])

root = PathTraversal()

class BoundPathTraversal(object):
    def __init__(self, node, path):
        self.__node = node
        if not isinstance(path, PathTraversal): path = PathTraversal(path)
        self.__path = path
    def __div__(self, other):
        return BoundPathTraversal( self.__node, self.__path.__div__(other) )
    __truediv__ = __div__
    def __repr__(self):
        return '%r / %r' % (self.__node, self.__path)
