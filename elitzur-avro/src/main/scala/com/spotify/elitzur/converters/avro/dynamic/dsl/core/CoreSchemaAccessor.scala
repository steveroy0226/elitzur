package com.spotify.elitzur.converters.avro.dynamic.dsl.core

import org.apache.avro.Schema

import scala.annotation.tailrec
import scala.collection.mutable
import java.{util => ju}

trait BaseObject {
  type Accessor = Any => Any

  private val mapToAccessor: mutable.Map[String, Accessor] = mutable.Map.empty[String, Accessor]

  def getFieldAccessor(fieldPath: String, schema: Schema): Accessor = {
    if (!mapToAccessor.contains(fieldPath)) {
      val accessorOps = getFieldAccessorOps(fieldPath, schema).map(_.ops)
      mapToAccessor += (fieldPath -> combineFns(accessorOps))
    }
    mapToAccessor(fieldPath)
  }

  @tailrec
  private[dsl] def getFieldAccessorOps(
    path: String,
    schema: Schema,
    accAccessorOps: List[AccessorOpsContainer] = List.empty[AccessorOpsContainer]
  ): List[AccessorOpsContainer] = {
    val currentAccessorOp = new CoreAccessorUtil().mapToAccessors(path, schema)
    val appAccessorOps = accAccessorOps :+ currentAccessorOp
    currentAccessorOp.rest match {
      case Some(remainingPath) =>
        getFieldAccessorOps(remainingPath, currentAccessorOp.schema, appAccessorOps)
      case _ => appAccessorOps
    }
  }

  private[dsl] def combineFns(fns: List[BaseAccessor]): Accessor =
    fns.map(_.fn).reduceLeftOption((f, g) => f andThen g).getOrElse(NoopAccessor().fn)
}

class CoreAccessorUtil {
  def isPrimitive(schema: Schema): Boolean = {
    val enumSet = ju.EnumSet.complementOf(
      ju.EnumSet.of(Schema.Type.ARRAY, Schema.Type.MAP, Schema.Type.UNION))
    enumSet.contains(schema.getType)
  }

  def isRepeated(schema: Schema): Boolean = {
    schema.getType == Schema.Type.ARRAY
  }

  def isNullable(schema: Schema): Boolean = {
    schema.getType == Schema.Type.UNION
  }

  def isNotSupported(schema: Schema): Boolean = {
    schema.getType == Schema.Type.MAP
  }

  def mapToAccessors(path: String, schema: Schema): AccessorOpsContainer = {
    val fieldTokens = pathToTokens(path)
    val fieldSchema = schema.getField(fieldTokens.field)

    mapToAccessors(fieldSchema.schema, fieldTokens)
  }

  def mapToAccessors(fieldSchema: Schema, fieldTokens: FieldTokens): AccessorOpsContainer = {
    fieldSchema match {
      case _schema if isPrimitive(_schema) =>
        new IndexAccessorLogic(fieldSchema, fieldTokens).accessorWithMetadata
//      case Schema.Type.ARRAY =>
//        new ArrayAccessorLogic(fieldSchema.getElementType, fieldTokens).avroOp
//      case Schema.Type.UNION =>
//        new NullableAccessorLogic(fieldSchema, fieldTokens).avroOp
      case _schema if isNotSupported(_schema) => throw new Exception("hello")
    }
  }

  private def pathToTokens(path: String): FieldTokens = {
    def strToOpt(str: String) :Option[String] = if (str.nonEmpty) Some(str) else None
    val token = '.'
    if (path.headOption.contains(token)) {
      val (fieldOps, rest) = path.drop(1).span(_ != token)
      val (field, op) = fieldOps.span(char => char.isLetterOrDigit || char == '_')
      FieldTokens(field, strToOpt(op), strToOpt(rest))
    } else {
      throw new Exception("hello")
    }
  }
}

case class AccessorOpsContainer(ops: BaseAccessor, schema: Schema, rest: Option[String])

case class FieldTokens(field: String, op: Option[String], rest: Option[String])
