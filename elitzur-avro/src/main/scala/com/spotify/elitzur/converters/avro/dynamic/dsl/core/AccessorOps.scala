/*
 * Copyright 2021 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.elitzur.converters.avro.dynamic.dsl.core

import com.spotify.elitzur.converters.avro.dynamic.dsl.DynamicImplicits.AccessorFunctionUtils

trait BaseAccessor {
  def fn: Any => Any
}

case class NoopAccessor() extends BaseAccessor {
  override def fn: Any => Any = (o: Any) => o
}

case class IndexAccessor(fieldFn: Any => Any) extends BaseAccessor {
  override def fn: Any => Any = fieldFn
}

case class NullableGenericAccessor(fieldFn: Any => Any, innerOps: List[BaseAccessor])
  extends BaseAccessor {
  override def fn: Any => Any = (o: Any) => {
    val fieldValue = fieldFn(o)
    val innerFn = innerOps.combineFns
    if (fieldValue == null) null else innerFn(o)
  }
}

case class NullableIndexAccessor(fieldFn: Any => Any, innerOps: List[BaseAccessor])
  extends BaseAccessor {
  override def fn: Any => Any = (o: Any) => {
    val fieldValue = fieldFn(o)
    if (fieldValue == null) null else innerOps.combineFns(fieldValue)
  }
}

//case class ArrayFlatmapAccessor(field: String, innerFn: Any => Any) extends BaseAccessor {
//  override def fn: Any => Any = (o: Any) => {
//    val innerAvroObj = o.asInstanceOf[GenericRecord].get(field)
//    val res = new ju.ArrayList[Any]
//    innerAvroObj.asInstanceOf[ju.List[Any]].forEach(
//      elem => innerFn(elem).asInstanceOf[ju.List[Any]].forEach( x => res.add(x)))
//    res
//  }
//}
//
//case class ArrayMapAccessor(field: String, innerFn: Any => Any) extends BaseAccessor {
//  override def fn: Any => Any = (o: Any) => {
//    val innerAvroObj = o.asInstanceOf[GenericRecord].get(field)
//    val res = new ju.ArrayList[Any]
//    innerAvroObj.asInstanceOf[ju.List[Any]].forEach(elem => res.add(innerFn(elem)))
//    res
//  }
//}
//
//case class ArrayNoopAccessor(field: String, flatten: Boolean) extends BaseAccessor {
//  override def fn: Any => Any = (o: Any) => IndexAccessor(field).fn(o)
//}
