package org.sheinbergon.dremio.udf.gis.spec

import com.dremio.exec.expr.SimpleFunction
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.test.TestScope
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.util.GeometryHelpers
import org.sheinbergon.dremio.udf.gis.util.release
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setFromWkt
import org.sheinbergon.dremio.udf.gis.util.valueIsAsDescribedInWKT
import org.sheinbergon.dremio.udf.gis.util.valueIsNotSet

abstract class GeometryProcessingFunSpec<F : SimpleFunction> : FunSpec() {

  protected fun testGeometryProcessing(
    name: String,
    wkt: String,
    expected: String,
    precursor: suspend TestScope.() -> Unit = {}
  ) = test(name) {
    precursor(this)
    function.apply {
      wkbInput.setFromWkt(wkt)
      setup()
      eval()
      wkbOutput.valueIsAsDescribedInWKT(expected)
    }
  }

  protected fun testNullGeometryProcessing(
    name: String,
    precursor: suspend TestScope.() -> Unit = {}
  ) = test(name) {
    precursor(this)
    function.apply {
      GeometryHelpers.markHolderNotSet(wkbInput)
      setup()
      eval()
      wkbOutput.valueIsNotSet()
    }
  }

  protected fun testInvalidGeometryProcessing(
    name: String,
    wkt: String,
    precursor: suspend TestScope.() -> Unit = {}
  ) = test(name) {
    precursor(this)
    function.apply {
      wkbInput.setFromWkt(wkt)
      setup()
      eval()
      wkbOutput.valueIsNotSet()
    }
  }

  init {
    beforeEach {
      function.wkbInput.reset()
      function.wkbOutput.reset()
    }

    afterEach {
      function.wkbInput.release()
    }
  }

  protected abstract val function: F
  protected abstract val F.wkbInput: NullableVarBinaryHolder
  protected abstract val F.wkbOutput: NullableVarBinaryHolder
}
