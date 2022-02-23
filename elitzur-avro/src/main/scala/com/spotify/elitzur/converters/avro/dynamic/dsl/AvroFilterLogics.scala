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

package com.spotify.elitzur.converters.avro.dynamic.dsl

import com.spotify.elitzur.converters.avro.dynamic.dsl.AvroMapperException._
import org.apache.avro.Schema

import scala.annotation.tailrec

trait BaseFilterLogic {
  val filter: BaseFilter
  val avroOp: AvroOperatorsHolder
}

class IndexFilterLogic(schema: Schema, pos: Int, rest: Option[String]) extends BaseFilterLogic {
  override val filter: BaseFilter = IndexFilter(pos)
  override val avroOp: AvroOperatorsHolder = AvroOperatorsHolder(filter, schema, rest)
}

class NullableFilterLogic(
  schema: Schema, pos: Int, firstFilter: AvroOperatorsHolder
  ) extends BaseFilterLogic {
  private final val nullableToken = "?"

  override val filter: BaseFilter = getNullableFilter(schema, pos, firstFilter)
  override val avroOp: AvroOperatorsHolder = AvroOperatorsHolder(filter, schema, None)

  private def getNullableFilter(
    innerSchema: Schema, pos: Int, firstFilter: AvroOperatorsHolder
  ): NullableFilter = {
    if (firstFilter.rest.isDefined) {
      val recursiveResult = AvroObjMapper.getAvroFilters(firstFilter.rest.get, innerSchema)
      // innerOps represents the list of all filters to be applied if the avro obj is not null
      val innerOps = (firstFilter +: recursiveResult).map(_.ops)
      NullableFilter(pos, innerOps, AvroObjMapper.combineFns(innerOps))
    } else {
      NullableFilter(pos, List(firstFilter.ops), firstFilter.ops.fn)
    }
  }
}

class ArrayFilterLogic(
  arrayElemSchema: Schema, pos: Int, rest: Option[String], op: Option[String]
  ) extends BaseFilterLogic {
  private final val arrayToken = "[]"

  override val filter: BaseFilter = getArrayFilter(arrayElemSchema, rest, op, pos)
  override val avroOp: AvroOperatorsHolder = AvroOperatorsHolder(filter, arrayElemSchema, None)

  private def getArrayFilter(
    innerSchema: Schema, remainingField: Option[String], opToken: Option[String], pos: Int
  ): BaseFilter = {
    if (remainingField.isDefined) {
      if (!opToken.contains(arrayToken)) throw new InvalidDynamicFieldException(MISSING_ARRAY_TOKEN)
      val recursiveResult = AvroObjMapper.getAvroFilters(remainingField.get, innerSchema)
      // innerOps represents the list of filters to be applied to each element in an array
      val innerOps = AvroObjMapper.combineFns(recursiveResult.map(_.ops))
      // flattenFlag is true if one of the internal operation types is a map based operation
      val flattenFlag = getFlattenFlag(recursiveResult.map(_.ops))
      if (flattenFlag) {
        ArrayFlatmapFilter(pos, innerOps)
      } else {
        ArrayMapFilter(pos, innerOps)
      }
    } else {
      ArrayNoopFilter(pos, opToken.contains(arrayToken))
    }
  }

  private def getFlattenFlag(ops: List[BaseFilter]): Boolean = {
    ops.foldLeft(false)((accBoolean, currFilter) => {
      val hasArrayFilter = currFilter match {
        case a: ArrayNoopFilter => a.flatten
        case n: NullableFilter => getFlattenFlag(n.innerOps)
        case _: ArrayMapFilter | _: ArrayFlatmapFilter => true
        case _ => false
      }
      accBoolean || hasArrayFilter
    })
  }
}
