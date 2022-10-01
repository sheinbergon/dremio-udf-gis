package org.sheinbergon.dremio.udf.gis.spec

import com.dremio.exec.expr.SimpleFunction
import io.kotest.core.spec.style.FunSpec
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.util.release
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setFromWkt
import org.sheinbergon.dremio.udf.gis.util.valueIsAsDescribedIn

abstract class GeometryOperatorsFunSpec<F : SimpleFunction> : FunSpec() {

  protected fun testGeometryOperator(
    name: String,
    wkt1: String,
    wkt2: String,
    expected: String
  ) = test(name) {
    function.apply {
      wkbInput1.setFromWkt(wkt1)
      wkbInput2.setFromWkt(wkt2)
      setup()
      eval()
      wkbOutput.valueIsAsDescribedIn(expected)
    }
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
