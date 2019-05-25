/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.blob.utils

import java.lang.reflect.Field

/**
  * Created by bluejoe on 2018/7/5.
  */
object ReflectUtils {
  implicit def reflected(o: AnyRef): ReflectedObject = new ReflectedObject(o);

  def singleton[T](implicit m: Manifest[T]): AnyRef = {
    val field = Class.forName(m.runtimeClass.getName + "$").getDeclaredField("MODULE$");
    field.setAccessible(true);
    field.get();
  }

  def instanceOf[T](args: Any*)(implicit m: Manifest[T]): T = {
    val constructor = m.runtimeClass.getDeclaredConstructor(args.map(_.getClass): _*);
    constructor.setAccessible(true);
    constructor.newInstance(args.map(_.asInstanceOf[Object]): _*).asInstanceOf[T];
  }

  def instanceOf(className: String)(args: Any*) = {
    val constructor = Class.forName(className).getDeclaredConstructor(args.map(_.getClass): _*);
    constructor.setAccessible(true);
    constructor.newInstance(args.map(_.asInstanceOf[Object]): _*);
  }
}

class ReflectedObject(o: AnyRef) {
  //employee._get("company.name")
  def _get(name: String): AnyRef = {
    try {
      var o2 = o;
      for (fn <- name.split("\\.")) {
        val field = _getField(o2.getClass, fn);
        field.setAccessible(true);
        o2 = field.get(o2);
      }
      o2;
    }
    catch {
      case e: NoSuchFieldException =>
        throw new InvalidFieldPathException(o, name, e);
    }
  }

  private def _getField(clazz: Class[_], fieldName: String): Field = {
    try {
      clazz.getDeclaredField(fieldName);
    }
    catch {
      case e: NoSuchFieldException =>
        val sc = clazz.getSuperclass;
        if (sc == null)
          throw e;

        _getField(sc, fieldName);
    }
  }

  def _getLazy(name: String): AnyRef = {
    _call(s"$name$$lzycompute")();
  }

  def _call(name: String)(args: Any*): AnyRef = {
    //val method = o.getClass.getDeclaredMethod(name, args.map(_.getClass): _*);
    //TODO: supports overloaded methods?
    val methods = o.getClass.getDeclaredMethods.filter(_.getName.equals(name));
    val method = methods(0);
    method.setAccessible(true);
    method.invoke(o, args.map(_.asInstanceOf[Object]): _*);
  }
}

class InvalidFieldPathException(o: AnyRef, path: String, cause: Throwable)
  extends RuntimeException(s"invalid field path: $path, host: $o", cause) {

}