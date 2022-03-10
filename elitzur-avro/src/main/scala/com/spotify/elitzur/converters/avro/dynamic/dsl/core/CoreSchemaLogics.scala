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
import com.spotify.elitzur.converters.avro.dynamic.dsl.core.AccessorExceptions._

abstract class BaseAccessorLogic[T: AvroOrBqSchema](schema: T, fieldTokens: FieldTokens) {
  val accessor: BaseAccessor
  val accessorWithMetadata: AccessorOpsContainer[T]
  def fieldAccessorFn: Any => Any =
    AvroOrBqSchemaUtil.getAvroOrBqFieldObject(schema, fieldTokens.field)
}

class IndexAccessorLogic[T: AvroOrBqSchema](schema: T, fieldTokens: FieldTokens)
  extends BaseAccessorLogic[T](schema, fieldTokens) {
  override val accessor: BaseAccessor = IndexAccessor(fieldAccessorFn)
  override val accessorWithMetadata: AccessorOpsContainer[T] =
    AccessorOpsContainer(accessor, schema, fieldTokens.rest)
}

class NullableAccessorLogic[T: AvroOrBqSchema](
  schema: T, fieldTokens: FieldTokens, innerAccessors: List[BaseAccessor]
) extends BaseAccessorLogic[T](schema, fieldTokens) {
  override val accessor: BaseAccessor = {
    if (innerAccessors.isEmpty) {
      IndexAccessor(fieldAccessorFn)
    } else {
      innerAccessors match {
        case head::tail if head.isInstanceOf[IndexAccessor] =>
          NullableIndexAccessor(fieldAccessorFn, tail.combineFns, tail)
        // TODO: Add logging here. Below code path exists for allowing nullable repeated fields,
        // which is supported by Avro. NullableGenericAccessor requires that the same field
        // accessor calculation to take place twice, which can be improved in the future.
        case _ =>
          NullableGenericAccessor(fieldAccessorFn, innerAccessors.combineFns, innerAccessors)
      }
    }
  }
  override val accessorWithMetadata: AccessorOpsContainer[T] =
    AccessorOpsContainer(accessor, schema, None)
}

class ArrayAccessorLogic[T: AvroOrBqSchema](
  schema: T, fieldTokens: FieldTokens, innerAccessors: List[BaseAccessor]
) extends BaseAccessorLogic[T](schema, fieldTokens) {
  private final val arrayToken = "[]"

  override val accessor: BaseAccessor = {
    if (innerAccessors.isEmpty) {
      ArrayNoopAccessor(fieldAccessorFn, fieldTokens.op.contains(arrayToken))
    } else {
      if (!fieldTokens.op.contains(arrayToken)) {
        throw new InvalidDynamicFieldException(MISSING_ARRAY_TOKEN)
      }
      val innerFn = innerAccessors.combineFns
      if (getFlattenFlag(innerAccessors)) {
        ArrayFlatmapAccessor(fieldAccessorFn, innerFn, innerAccessors)
      } else {
        ArrayMapAccessor(fieldAccessorFn, innerFn, innerAccessors)
      }
    }
  }
  override val accessorWithMetadata: AccessorOpsContainer[T] =
    AccessorOpsContainer(accessor, schema, None)

//  private def getArrayAccessor(innerSchema: T, fieldTokens: FieldTokens): BaseAccessor = {
//    if (fieldTokens.rest.isDefined) {
//
//      val recursiveResult = AvroObjMapper.getAvroAccessors(fieldTokens.rest.get, innerSchema)
//      // innerOps represents the list of accessors to be applied to each element in an array
//      val innerOps = AvroObjMapper.combineFns(recursiveResult.map(_.ops))
//      // flattenFlag is true if one of the internal operation types is a map based operation
//      val flattenFlag = getFlattenFlag(recursiveResult.map(_.ops))
//      if (flattenFlag) {
//        ArrayFlatmapAccessor(fieldTokens.field, innerOps)
//      } else {
//        ArrayMapAccessor(fieldTokens.field, innerOps)
//      }
//    } else {
//      ArrayNoopAccessor(fieldTokens.field, fieldTokens.op.contains(arrayToken))
//    }
//  }
//
  private def getFlattenFlag(ops: List[BaseAccessor]): Boolean = {
    ops.foldLeft(false)((accBoolean, currAccessor) => {
      val hasArrayAccessor = currAccessor match {
        case n: NullableIndexAccessor => getFlattenFlag(n.innerOps)
        case n: NullableGenericAccessor => getFlattenFlag(n.innerOps)
        case a: ArrayNoopAccessor => a.flatten
        case _: ArrayMapAccessor | _: ArrayFlatmapAccessor => true
        case _ => false
      }
      accBoolean || hasArrayAccessor
    })
  }
}
