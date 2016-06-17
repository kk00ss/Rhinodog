// Copyright 2016 Konstantin Voznesenskiy

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package test.scala

import org.junit._
import rhinodog.Core.Definitions._
import rhinodog.Core.MeasureFormats._
import rhinodog.Core.Utils.DocumentSerializer

class AnalyzedDocumentSerializerTest {
    @Test
    def test() = {
        val measureSerializer = new OkapiBM25MeasureSerializer()
        val serializer = new DocumentSerializer(measureSerializer)
        val doc = AnalyzedDocument("test test test",
            Array(DocTerm(1,OkapiBM25Measure(1,100)), DocTerm(2, OkapiBM25Measure(2, 100)), DocTerm(3, OkapiBM25Measure(3, 100))))
        val serializedDoc = serializer.serialize(doc)
        val deserializedDoc = serializer.deserialize(serializedDoc)
        assert(doc == deserializedDoc)
    }
}
