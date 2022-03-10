package com.spotify.elitzur.converters.avro.dynamic.dsl.bq

import com.spotify.elitzur.converters.avro.dynamic.dsl.core.CoreAccessorUtil

object BqAccessorUtil extends CoreAccessorUtil[BqSchema] {
  override def isPrimitive(fieldSchema: BqSchema): Boolean = {
    fieldSchema.fieldMode == BqField.Required
  }

  override def isRepeated(fieldSchema: BqSchema): Boolean = {
    fieldSchema.fieldMode == BqField.Repeated
  }

  override def isNullable(fieldSchema: BqSchema): Boolean = {
    fieldSchema.fieldMode == BqField.Nullable
  }

  override def isNotSupported(fieldSchema: BqSchema): Boolean = {
    throw new Exception("Not Possible")
  }

  override def getFieldSchema(schema: BqSchema, fieldName: String): BqSchema = {
    val fieldSchema = schema.fieldSchemas.stream()
      .filter(_.getName == fieldName)
      .findFirst()
      .orElseThrow(() => throw new Exception("the field doesn't exist!!!"))
    BqSchema(fieldSchema.getFields, fieldSchema.getMode)
  }

  // TODO: review this logic
  override def getNonNullableFieldSchema(schema: BqSchema): BqSchema = {
    schema.copy(fieldMode = BqField.Required)
  }
}
