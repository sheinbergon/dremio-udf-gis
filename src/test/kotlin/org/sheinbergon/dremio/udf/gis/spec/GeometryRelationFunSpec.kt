package org.sheinbergon.dremio.udf.gis.spec

import com.dremio.exec.expr.SimpleFunction
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.ints.shouldBeExactly
import org.apache.arrow.vector.holders.BitHolder
import org.apache.arrow.vector.holders.NullableVarBinaryHolder
import org.sheinbergon.dremio.udf.gis.util.release
import org.sheinbergon.dremio.udf.gis.util.reset
import org.sheinbergon.dremio.udf.gis.util.setFromWkt


abstract class GeometryRelationFunSpec<F : SimpleFunction> : FunSpec() {

    init {
        beforeEach {
            function.wkbInput1.reset()
            function.wkbInput2.reset()
            function.output.reset()
        }

        afterEach {
            function.wkbInput1.release()
        }
    }

    protected fun testTrueGeometryRelation(
        name: String,
        wkt1: String,
        wkt2: String
    ) = test(name) {
        function.apply {
            wkbInput1.setFromWkt(wkt1)
            wkbInput2.setFromWkt(wkt2)
            setup()
            eval()
            output.valueIsTrue()
        }
    }

    protected fun testFalseGeometryRelation(
        name: String,
        wkt1: String,
        wkt2: String
    ) = test(name) {
        function.apply {
            wkbInput1.setFromWkt(wkt1)
            wkbInput2.setFromWkt(wkt2)
            setup()
            eval()
            output.valueIsFalse()
        }
    }

    private fun BitHolder.valueIsTrue() = this.value shouldBeExactly 1
    private fun BitHolder.valueIsFalse() = this.value shouldBeExactly 0

    protected abstract val function: F
    protected abstract val F.wkbInput1: NullableVarBinaryHolder
    protected abstract val F.wkbInput2: NullableVarBinaryHolder
    protected abstract val F.output: BitHolder
}
