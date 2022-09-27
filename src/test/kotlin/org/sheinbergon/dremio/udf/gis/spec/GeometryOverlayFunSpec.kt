package org.sheinbergon.dremio.udf.gis.spec

import com.dremio.exec.expr.SimpleFunction
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.locationtech.jts.io.WKTWriter
import org.sheinbergon.dremio.udf.gis.util.GeometryHelpers
import org.sheinbergon.dremio.udf.gis.util.release
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setFromWkt
import org.sheinbergon.dremio.udf.gis.util.setUtf8

abstract class GeometryOverlayFunSpec<F : SimpleFunction> : FunSpec() {

  protected fun testGeometryOverlay(
    name: String,
    wkt1: String,
    wkt2: String,
    result: String
  ) = test(name) {
    function.apply {
      wkbInput1.setFromWkt(wkt1)
      wkbInput2.setFromWkt(wkt2)
      setup()
      eval()
      wkbOutput.valueIsAsDescribedInText(result)
    }
  }

  private fun NullableVarBinaryHolder.valueIsAsDescribedInText(text: String) {
    val evaluated = GeometryHelpers.toGeometry(this)
    println(WKTWriter().write(evaluated))
    val expected = NullableVarCharHolder()
      .apply { setUtf8(text) }
      .let(GeometryHelpers::toGeometry)
    GeometryHelpers.toBinary(evaluated) shouldBe GeometryHelpers.toBinary(expected)
  }

  init {
    beforeEach {
      function.wkbInput1.reset()
      function.wkbInput2.reset()
      function.wkbOutput.reset()
    }

    afterEach {
      function.wkbInput1.release()
      function.wkbInput2.release()
    }
  }

  protected abstract val function: F

  protected abstract val F.wkbInput1: NullableVarBinaryHolder
  protected abstract val F.wkbInput2: NullableVarBinaryHolder
  protected abstract val F.wkbOutput: NullableVarBinaryHolder
}
