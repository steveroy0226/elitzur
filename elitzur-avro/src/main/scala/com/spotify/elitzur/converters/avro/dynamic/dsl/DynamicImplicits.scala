package com.spotify.elitzur.converters.avro.dynamic.dsl

import com.spotify.elitzur.converters.avro.dynamic.dsl.core.{BaseAccessor, NoopAccessor}

object DynamicImplicits {
  implicit class AccessorFunctionUtils(val fns: List[BaseAccessor]) {
    private[dsl] def combineFns: Any => Any =
      fns.map(_.fn).reduceLeftOption((f, g) => f andThen g).getOrElse(NoopAccessor().fn)
  }
}
