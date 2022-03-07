package com.spotify.elitzur.converters.avro.dynamic.dsl.core

import org.apache.avro.Schema
import com.google.api.services.bigquery.model.TableFieldSchema

sealed trait AvroOrBqSchema[T]

object AvroOrBqSchema {
  implicit val avroSchema: AvroOrBqSchema[Schema] =
    new AvroOrBqSchema[Schema] {}
  implicit val bqSchema: AvroOrBqSchema[TableFieldSchema] =
    new AvroOrBqSchema[TableFieldSchema] {}
}

object Thing {
  def getAvroOrBqObject[T: AvroOrBqSchema](t: T): String = t match {
    case avro: Schema => "avro"
    case bq: TableFieldSchema => "bq"
  }
}
