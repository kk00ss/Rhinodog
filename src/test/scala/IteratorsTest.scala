package test.scala

import org.junit._
import rhinodog.Core.Iterators._
import rhinodog.Core.Definitions.DocPosting
import rhinodog.Core.MeasureFormats.MeasureSimple

class IteratorsTest {

    val data = Array(
        DocPosting(1, MeasureSimple(1, 100)),
        DocPosting(2, MeasureSimple(5, 100)),
        DocPosting(3, MeasureSimple(1, 100)),
        DocPosting(4, MeasureSimple(7, 100)),
        DocPosting(5, MeasureSimple(1, 100)),
        DocPosting(6, MeasureSimple(8, 100)),
        DocPosting(7, MeasureSimple(5, 100)),
        DocPosting(8, MeasureSimple(1, 100)),
        DocPosting(9, MeasureSimple(10, 100)),  //max
        DocPosting(10, MeasureSimple(5, 100)))

    @Test
    def SegmentIteratorTest() = {
//        val iterator1 = new SegmentIterator[MeasureSimple](data)
//        assert(iterator1.current.get == DocPosting(1, MeasureSimple(1, 100)))
//
//        val second = iterator1.next().get
//        assert(second == DocPosting(2, MeasureSimple(5, 100)))
//
//        val largerThanSecond = iterator1.next(second.measure).get
//        assert(largerThanSecond.measure.compare(second.measure) > 0)
//        assert(largerThanSecond == DocPosting(4, MeasureSimple(7, 100)))
//        assert(iterator1.next(MeasureSimple(5,20)).isEmpty)
//
//        val iterator2 = new SegmentIterator[MeasureSimple](data)
//        val advanceRes1 = iterator2.advance(5)
//        assert(advanceRes1.get == DocPosting(5, MeasureSimple(1, 100)))
//
//        val advanceRes2 = iterator2.advance(5)
//        assert(advanceRes2.get == DocPosting(6, MeasureSimple(8, 100)))
//
//        var last = iterator2.next()
//        while (iterator2.next().isDefined){ }
//        last = iterator2.current
//        assert(last.get == DocPosting(10, MeasureSimple(5, 100)))
//        assert(iterator2.next().isEmpty)
//        assert(iterator2.next(MeasureSimple(9,100)).isEmpty)
//        assert(iterator2.advance(100).isEmpty)

    }
}
