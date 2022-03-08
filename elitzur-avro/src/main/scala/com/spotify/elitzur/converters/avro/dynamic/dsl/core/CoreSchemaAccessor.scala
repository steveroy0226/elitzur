package com.spotify.elitzur.converters.avro.dynamic.dsl.core

import com.google.api.services.bigquery.model.TableSchema
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.annotation.tailrec
import scala.collection.mutable
import java.{util => ju}

class BaseObject[T: AvroOrBqSchema](schema: T) {
  type Accessor = Any => Any

  private val mapToAccessor: mutable.Map[String, Accessor] = mutable.Map.empty[String, Accessor]

  def getFieldAccessor(fieldPath: String): Accessor = {
    if (!mapToAccessor.contains(fieldPath)) {
      val accessorOps = schema match {
          case avro: Schema => AvroAccessorUtil.getFieldAccessorOps(fieldPath, avro).map(_.ops)
          case _ => throw new Exception("???")
        }
      mapToAccessor += (fieldPath -> combineFns(accessorOps))
    }
    mapToAccessor(fieldPath)
  }

  private[dsl] def combineFns(fns: List[BaseAccessor]): Accessor =
    fns.map(_.fn).reduceLeftOption((f, g) => f andThen g).getOrElse(NoopAccessor().fn)
}

object AvroAccessorUtil extends CoreAccessorUtil[Schema]() {
  override def isPrimitive(fieldSchema: Schema): Boolean = {
    val enumSet = ju.EnumSet.complementOf(
      ju.EnumSet.of(Schema.Type.ARRAY, Schema.Type.MAP, Schema.Type.UNION))
    enumSet.contains(fieldSchema.getType)
  }

  override def isRepeated(fieldSchema: Schema): Boolean = {
    fieldSchema.getType == Schema.Type.ARRAY
  }

  override def isNullable(fieldSchema: Schema): Boolean = {
    fieldSchema.getType == Schema.Type.UNION
  }

  override def isNotSupported(fieldSchema: Schema): Boolean = {
    fieldSchema.getType == Schema.Type.MAP
  }

  override def getFieldSchema(schema: Schema, fieldSchema: String): Schema = {
    schema.getField(fieldSchema).schema()
  }

}

abstract class CoreAccessorUtil[T: AvroOrBqSchema]() {
  def isPrimitive(fieldSchema: T): Boolean

  def isRepeated(fieldSchema: T): Boolean

  def isNullable(fieldSchema: T): Boolean

  def isNotSupported(fieldSchema: T): Boolean

  def getFieldSchema(schema: T, fieldSchema: String): T

  def mapToAccessors(path: String, parentSchema: T): AccessorOpsContainer[T] = {
    val fieldTokens = pathToTokens(path)
    val fieldSchema = getFieldSchema(parentSchema, fieldTokens.field)

    mapToAccessors(fieldSchema, fieldTokens)
  }

  def mapToAccessors(fieldSchema: T, fieldTokens: FieldTokens): AccessorOpsContainer[T] = {
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

  @tailrec
  private[dsl] final def getFieldAccessorOps(
    fieldPath: String,
    parentSchema: T,
    accAccessorOps: List[AccessorOpsContainer[T]] = List.empty[AccessorOpsContainer[T]]
  ): List[AccessorOpsContainer[T]] = {
    val currentAccessorOp = mapToAccessors(fieldPath, parentSchema)
    val appAccessorOps = accAccessorOps :+ currentAccessorOp
    currentAccessorOp.rest match {
      case Some(remainingPath) =>
        getFieldAccessorOps(remainingPath, currentAccessorOp.schema, appAccessorOps)
      case _ => appAccessorOps
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

case class AccessorOpsContainer[T: AvroOrBqSchema](
  ops: BaseAccessor, schema: T, rest: Option[String])

case class FieldTokens(field: String, op: Option[String], rest: Option[String])
