package com.spotify.elitzur.converters.avro.dynamic.dsl.avro

import com.spotify.elitzur.converters.avro.dynamic.dsl.core.CoreAccessorUtil
import org.apache.avro.Schema

import java.{util => ju}

object AvroAccessorUtil extends CoreAccessorUtil[Schema] {
  override def isPrimitive(fieldSchema: Schema): Boolean = {
    val enumSet = ju.EnumSet.complementOf(
      ju.EnumSet.of(Schema.Type.ARRAY, Schema.Type.MAP, Schema.Type.UNION)
    )
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

  override def getFieldSchema(schema: Schema, fieldName: String): Schema = {
    schema.getField(fieldName).schema()
  }
}
