package test.scala

import org.junit._
import rhinodog.Core.Definitions._
import rhinodog.Core.MeasureFormats._
import rhinodog.Core.Utils.DocumentSerializer

class AnalyzedDocumentSerializerTest {
    @Test
    def test() = {
        val measureSerializer = new MeasureSerializerSimple()
        val serializer = new DocumentSerializer(measureSerializer)
        val doc = AnalyzedDocument("test test test",
            Array(DocTerm(1,MeasureSimple(1,100)), DocTerm(2, MeasureSimple(2, 100)), DocTerm(3, MeasureSimple(3, 100))))
        val serializedDoc = serializer.serialize(doc)
        val deserializedDoc = serializer.deserialize(serializedDoc)
        assert(doc == deserializedDoc)
    }
}
