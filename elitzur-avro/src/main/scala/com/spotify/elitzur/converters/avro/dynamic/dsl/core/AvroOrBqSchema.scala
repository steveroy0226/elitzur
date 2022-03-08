package com.spotify.elitzur.converters.avro.dynamic.dsl.core

import org.apache.avro.Schema
import com.google.api.services.bigquery.model.TableSchema
import com.google.api.services.bigquery.model.TableRow
import org.apache.avro.generic.GenericRecord

sealed trait AvroOrBqSchema[T]

object AvroOrBqSchema {
  implicit val avroSchema: AvroOrBqSchema[Schema] =
    new AvroOrBqSchema[Schema] {}
  implicit val bqSchema: AvroOrBqSchema[TableSchema] =
    new AvroOrBqSchema[TableSchema] {}
}

object AvroOrBqSchemaUtil {
  def getAvroOrBqFieldObject[T: AvroOrBqSchema](t: T): (Object, String) => Object = t match {
    case _: Schema => (o: Object, fieldName: String) => o.asInstanceOf[GenericRecord].get(fieldName)
    case _: TableSchema => (o: Object, fieldName: String) => o
  }
}
