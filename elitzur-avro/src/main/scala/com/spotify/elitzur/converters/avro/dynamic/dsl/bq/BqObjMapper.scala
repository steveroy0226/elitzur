///*
// * Copyright 2021 Spotify AB.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//package com.spotify.elitzur.converters.avro.dynamic.dsl.bq
//
//import com.spotify.elitzur.converters.avro.dynamic.dsl.avro.AvroAccessorException._
//import com.google.api.services.bigquery.model.TableFieldSchema
//import com.spotify.elitzur.converters.avro.dynamic.dsl.avro.BaseAccessor
//
//import java.{util => ju}
//import scala.annotation.tailrec
//import scala.collection.mutable
//
//object BqObjMapper {
//  private val mapToAvroFun: mutable.Map[String, Any => Any] = mutable.Map.empty[String, Any => Any]
//
//  def getAvroFun(avroFieldPath: String, schema: TableFieldSchema): Any => Any = {
//    if (!mapToAvroFun.contains(avroFieldPath)) {
//      val avroOperators = getBqAccessor(avroFieldPath, schema).map(_.ops)
//      mapToAvroFun += (avroFieldPath -> combineFns(avroOperators))
//    }
//    mapToAvroFun(avroFieldPath)
//  }
//
//  @tailrec
//  private[dsl] def getBqAccessor(
//    path: String,
//    schema: TableFieldSchema,
//    accAvroOperators: List[BqAccessorContainer] = List.empty[BqAccessorContainer]
//  ): List[BqAccessorContainer] = {
//    val thisAvroOp = BqAccessorUtil.mapToAccessors(path, schema)
//    val appendedAvroOp = accAvroOperators :+ thisAvroOp
//    thisAvroOp.rest match {
//      case Some(remainingPath) => getBqAccessor(remainingPath, thisAvroOp.schema, appendedAvroOp)
//      case _ => appendedAvroOp
//    }
//  }
//
//  private[dsl] def combineFns(fns: List[BaseAccessor]): Any => Any =
//    fns.map(_.fn).reduceLeftOption((f, g) => f andThen g).getOrElse(NoopAccessor().fn)
//}
//
//object BqAccessorUtil {
//  private val PRIMITIVES: ju.EnumSet[TableFieldSchema.Type] =
//    ju.EnumSet.complementOf(ju.EnumSet.of(Schema.Type.ARRAY, Schema.Type.MAP, Schema.Type.UNION))
//
//  def mapToAccessors(path: String, schema: TableFieldSchema): BqAccessorContainer = {
//    val fieldTokens = pathToTokens(path)
//    val fieldSchema = schema.getField(fieldTokens.field)
//
//    mapToAccessors(fieldSchema.schema, fieldTokens)
//  }
//
//  def mapToAccessors(
//    fieldSchema: TableFieldSchema, fieldTokens: BqFieldTokens): BqAccessorContainer = {
//    fieldSchema.getType match {
//      case _ => "abc"
////      case _schema if PRIMITIVES.contains(_schema) =>
////        new IndexAccessorLogic(fieldSchema, fieldTokens).avroOp
////      case Schema.Type.ARRAY =>
////        new ArrayAccessorLogic(fieldSchema.getElementType, fieldTokens).avroOp
////      case Schema.Type.UNION =>
////        new NullableAccessorLogic(fieldSchema, fieldTokens).avroOp
////      case Schema.Type.MAP => throw new InvalidDynamicFieldException(UNSUPPORTED_MAP_SCHEMA)
//    }
//  }
//
//  private def pathToTokens(path: String): BqFieldTokens = {
//    def strToOpt(str: String) :Option[String] = if (str.nonEmpty) Some(str) else None
//    val token = '.'
//    if (path.headOption.contains(token)) {
//      val (fieldOps, rest) = path.drop(1).span(_ != token)
//      val (field, op) = fieldOps.span(char => char.isLetterOrDigit || char == '_')
//      BqFieldTokens(field, strToOpt(op), strToOpt(rest))
//    } else {
//      throw new InvalidDynamicFieldException(MISSING_TOKEN)
//    }
//  }
//}
//
//case class BqAccessorContainer(ops: BaseAccessor, schema: TableFieldSchema, rest: Option[String])
//
//case class BqFieldTokens(field: String, op: Option[String], rest: Option[String])
