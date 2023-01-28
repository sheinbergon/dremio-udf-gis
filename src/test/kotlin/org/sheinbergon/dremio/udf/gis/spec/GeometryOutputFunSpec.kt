package org.sheinbergon.dremio.udf.gis.spec

import com.dremio.exec.expr.SimpleFunction
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.apache.arrow.vector.holders.NullableVarCharHolder
import org.apache.arrow.vector.holders.ValueHolder
import org.sheinbergon.dremio.udf.gis.util.GeometryHelpers
import org.sheinbergon.dremio.udf.gis.util.release
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setFromWkt
import org.sheinbergon.dremio.udf.gis.util.valueIsAsDescribedInEWKT
import org.sheinbergon.dremio.udf.gis.util.valueIsAsDescribedInWKT
import org.sheinbergon.dremio.udf.gis.util.valueIsNotSet

abstract class GeometryOutputFunSpec<F : SimpleFunction, O : ValueHolder> : FunSpec() {

  abstract class NullableVarBinary<F : SimpleFunction> : GeometryOutputFunSpec<F, NullableVarBinaryHolder>() {
    init {
      beforeEach {
        function.output.reset()
      }
    }

    protected fun testGeometryWKTOutput(
      name: String,
      wkt: String,
      expected: String
    ) = test(name) {
      function.apply {
        wkbInput.setFromWkt(wkt)
        setup()
        eval()
        output.valueIsAsDescribedInWKT(expected)
      }
    }

    protected fun testGeometryEWKTOutput(
      name: String,
      wkt: String,
      srid: Int,
      expected: String
    ) = test(name) {
      function.apply {
        wkbInput.setFromWkt(wkt, srid)
        setup()
        eval()
        output.valueIsAsDescribedInEWKT(expected)
      }
    }

    protected fun testNullGeometryOutput(
      name: String
    ) = test(name) {
      function.apply {
        wkbInput.isSet = 0
        setup()
        eval()
        output.valueIsNotSet()
      }
    }
  }

  abstract class NullableVarChar<F : SimpleFunction> : GeometryOutputFunSpec<F, NullableVarCharHolder>() {
    init {
      beforeEach {
        function.output.reset()
      }
    }

    protected fun testGeometryOutput(
      name: String,
      wkt: String,
      srid: Int,
      expected: String
    ) = test(name) {
      function.apply {
        wkbInput.setFromWkt(wkt, srid)
        setup()
        eval()
        output.valueIs(expected)
      }
    }

    protected fun testNullGeometryOutput(
      name: String
    ) = test(name) {
      function.apply {
        wkbInput.isSet = 0
        setup()
        eval()
        output.valueIsNotSet()
      }
    }

    private fun NullableVarCharHolder.valueIs(text: String) =
      GeometryHelpers.toUTF8String(this) shouldBe text
  }

  init {
    beforeEach {
      function.wkbInput.reset()
    }

    afterEach {
      function.wkbInput.release()
    }
  }

  protected abstract val function: F
  protected abstract val F.wkbInput: NullableVarBinaryHolder
  protected abstract val F.output: O
}
