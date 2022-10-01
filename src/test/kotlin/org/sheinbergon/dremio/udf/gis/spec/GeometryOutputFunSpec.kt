package org.sheinbergon.dremio.udf.gis.spec

import com.dremio.exec.expr.SimpleFunction
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.sheinbergon.dremio.udf.gis.util.GeometryHelpers
import org.sheinbergon.dremio.udf.gis.util.release
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setFromWkt

abstract class GeometryOutputFunSpec<F : SimpleFunction> : FunSpec() {

  init {
    beforeEach {
      function.output.reset()
      function.wkbInput.reset()
    }

    afterEach {
      function.wkbInput.release()
    }
  }

  protected fun testGeometryOutput(
    name: String,
    wkt: String,
    srid: Int,
    result: String
  ) = test(name) {
    function.apply {
      wkbInput.setFromWkt(wkt, srid)
      setup()
      eval()
      output.valueIs(result)
    }
  }

  private fun NullableVarCharHolder.valueIs(text: String) =
    GeometryHelpers.toUTF8String(this) shouldBe text

  protected abstract val function: F
  protected abstract val F.wkbInput: NullableVarBinaryHolder
  protected abstract val F.output: NullableVarCharHolder
}
