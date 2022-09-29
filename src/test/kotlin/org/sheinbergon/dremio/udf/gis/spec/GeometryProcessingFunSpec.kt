package org.sheinbergon.dremio.udf.gis.spec

import com.dremio.exec.expr.SimpleFunction
import io.kotest.core.spec.style.FunSpec
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.util.release
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setFromWkt
import org.sheinbergon.dremio.udf.gis.util.valueIsAsDescribedIn

abstract class GeometryProcessingFunSpec<F : SimpleFunction> : FunSpec() {

  protected fun testGeometryProcessing(
    name: String,
    wkt: String,
    expected: String
  ) = test(name) {
    function.apply {
      wkbInput.setFromWkt(wkt)
      setup()
      eval()
      wkbOutput.valueIsAsDescribedIn(expected)
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
