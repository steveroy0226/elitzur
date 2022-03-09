package com.spotify.elitzur.converters.avro.dynamic.dsl.core

import scala.annotation.tailrec

abstract class CoreAccessorUtil[T: AvroOrBqSchema] {
  def isPrimitive(fieldSchema: T): Boolean

  def isRepeated(fieldSchema: T): Boolean

  def isNullable(fieldSchema: T): Boolean

  def isNotSupported(fieldSchema: T): Boolean

  def getFieldSchema(schema: T, fieldName: String): T

  def getNonNullableFieldSchema(schema: T): T

  def mapToAccessors(path: String, parentSchema: T): AccessorOpsContainer[T] = {
    val fieldTokens = FieldTokens(path)
    val fieldSchema = getFieldSchema(parentSchema, fieldTokens.field)

    mapToAccessors(fieldSchema, fieldTokens)
  }

  private def mapToAccessors(
    fieldSchema: T, fieldTokens: FieldTokens
  ): AccessorOpsContainer[T] = {
    fieldSchema match {
      case _schema if isPrimitive(_schema) =>
        new IndexAccessorLogic(_schema, fieldTokens).accessorWithMetadata
      //      case Schema.Type.ARRAY =>
      //        new ArrayAccessorLogic(fieldSchema.getElementType, fieldTokens).avroOp
      case _schema if isNullable(_schema) =>
        val nonNullSchema = getNonNullableFieldSchema(_schema)
        val currOp = mapToAccessors(nonNullSchema, fieldTokens).ops
        val restOp = getInnerOps(nonNullSchema, fieldTokens.rest)
        val allOps = currOp :: restOp
        new NullableAccessorLogic(nonNullSchema, fieldTokens, allOps).accessorWithMetadata
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

  private def getInnerOps(childSchema: T, remainingPath: Option[String]): List[BaseAccessor] = {
    if (remainingPath.isDefined) {
      getFieldAccessorOps(remainingPath.get, childSchema).map(_.ops)
    } else {
      List.empty[BaseAccessor]
    }
  }
}

case class AccessorOpsContainer[T: AvroOrBqSchema](
  ops: BaseAccessor, schema: T, rest: Option[String])
