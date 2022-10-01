package org.sheinbergon.dremio.udf.gis.spec

import com.dremio.exec.expr.SimpleFunction
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.test.TestScope
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.util.release
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setFromWkt
import org.sheinbergon.dremio.udf.gis.util.valueIsAsDescribedIn

abstract class GeometryTransformationFunSpec<F : SimpleFunction> : FunSpec() {

  protected fun testGeometryTransformation(
    name: String,
    wkt: String,
    sourceSrid: Int? = null,
    expected: String,
    precursor: suspend TestScope.() -> Unit = {}
  ) = test(name) {
    precursor.invoke(this)
    function.apply {
      wkbInput.setFromWkt(wkt, sourceSrid)
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
