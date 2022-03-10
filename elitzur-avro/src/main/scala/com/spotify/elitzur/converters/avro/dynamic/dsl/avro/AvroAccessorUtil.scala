package com.spotify.elitzur.converters.avro.dynamic.dsl.avro

import com.spotify.elitzur.converters.avro.dynamic.dsl.core.AccessorExceptions._
import com.spotify.elitzur.converters.avro.dynamic.dsl.core.CoreAccessorUtil
import org.apache.avro.Schema

import java.{util => ju}

object AvroAccessorUtil extends CoreAccessorUtil[Schema] {
  private val PRIMITIVE_SET = ju.EnumSet.complementOf(
    ju.EnumSet.of(Schema.Type.ARRAY, Schema.Type.MAP, Schema.Type.UNION)
  )

  override def isPrimitive(fieldSchema: Schema): Boolean = {
    PRIMITIVE_SET.contains(fieldSchema.getType)
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

  override def getFieldSchema(schema: Schema, fieldName: String): Schema = {
    schema.getField(fieldName).schema()
  }

  override def getNonNullableFieldSchema(schema: Schema): Schema = {
    val nonNullSchemas: ju.ArrayList[Schema] = new ju.ArrayList[Schema]
    schema.getTypes.forEach(s => if (s.getType != Schema.Type.NULL) {nonNullSchemas.add(s)})
    if (nonNullSchemas.size > 1) {
      throw new InvalidDynamicFieldException(INVALID_UNION_SCHEMA)
    }
    nonNullSchemas.get(0)
  }

  override def getElemFieldSchema(schema: Schema): Schema = schema.getElementType

}
